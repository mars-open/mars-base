/*
 * MARS Base - Maintenance Applications for Railway Systems
 *
 * Copyright © 2026 zzeekk (<zach.kull@gmail.com>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package ch.zzeekk.mars.pp

import ch.zzeekk.mars.pp.utils.GeometryCalcUtils.convert4326to3857
import ch.zzeekk.mars.pp.utils.SparkUtils.jsonifyComplexDataTypes
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.dataobject.{CanWriteSparkDataFrame, DataObject, DataObjectMetadata}
import io.tileverse.pmtiles.{CompressionUtil, PMTilesHeader, TileverseAccessor}
import io.tileverse.tiling.common.{Coordinate => TLCoordinate}
import io.tileverse.tiling.matrix.{DefaultTileMatrixSets, TileMatrix}
import io.tileverse.tiling.pyramid.TileIndex
import io.tileverse.vectortile.mvt.{VectorTileBuilder, VectorTileCodec}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.util.AffineTransformation

import java.io.File
import java.nio.file.Files
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Creates a PMTile files containing Vector Tiles for the geometries of the DataFrame.
 * Each record is treated as feature and needs a geometry column.
 * The DataFrame
 * - needs a column with name "geometry", having a Sedona Geometry DataType
 * - needs a column with name "layer", containing the layer name for this feature
 * - can have a column with name "id" with type "long", used as feature id
 *
 * Note that geometries are expected in WGS84 / EPSG:4326 coordinate reference system.
 *
 * @param compressTiles if true tiles are compressed using gzip.
 *                      Unfortunately this doesnt seem to work with maplibre online, but it works when accessing a local file (pmtiles.io viewer)
 */
case class PMTilesDataObject(
                              id: DataObjectId,
                              zoomAndFilter: Map[String, String],
                              localPath: String,
                              path: Option[String],
                              colsToIgnore: Seq[String] = Seq(),
                              compressTiles: Boolean = false,
                              metadata: Option[DataObjectMetadata] = None
                            )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanWriteSparkDataFrame {

  val hadoopPath: Option[Path] = path.map(HdfsUtil.prefixHadoopPath(_, None))

  def filesystem(implicit context: ActionPipelineContext): FileSystem = {
    if (filesystemHolder == null && hadoopPath.isDefined) {
      filesystemHolder = HdfsUtil.getHadoopFsWithConf(hadoopPath.get)(context.hadoopConf)
    }
    filesystemHolder
  }
  @transient private var filesystemHolder: FileSystem = _

  /**
   * @param geom geometry object using EPSG 4326 coordinates
   */
  def getTileZXY(zoom: Int, geom: Geometry): TileId = {
    val tm = PMTilesDataObject.getPmTileMatrix(zoom)
    // TileMatrix works best with "web mercator", e.g. EPSG 3857
    val coord = convert4326to3857(geom).getCoordinate
    val tile = tm.coordinateToTile(new TLCoordinate(coord.x, coord.y)).get()
    TileId(tile.z(), tile.x(), tile.y())
  }

  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap = {
    implicit val session: SparkSession = df.sparkSession
    import session.implicits._

    assert(df.columns.contains("geometry"))
    assert(df.columns.contains("layer"))

    val dfSimplified = jsonifyComplexDataTypes(df)

    // Explode zoom levels
    val dfWithTileZ = zoomAndFilter.view.mapValues(expr).map {
      case (zoom, filter) =>
        val udfGetTileZXY = udf(getTileZXY _)
        dfSimplified
          .where(filter)
          .withColumn("tile", udfGetTileZXY(lit(zoom.toInt), $"geometry"))
    }.reduce(_.unionAll(_))

    // create MVT-Tiles
    val udfCreateMvtTile = udf(createMvtTile(compressTiles) _)
    val dsTile = dfWithTileZ
      .groupBy($"tile")
      .agg(collect_list(struct(df.columns.map(col):_*)).as("features"))
      .withColumn("data", udfCreateMvtTile($"features", $"tile.z", $"tile.x", $"tile.y"))
      .select($"tile", $"data")
      .as[TileData]

    // create local PMTiles file
    logger.info(s"Creating local pmtiles file at $localPath")
    val localFile = new File(localPath)
    localFile.delete()
    assert(!localFile.exists(), s"Could not delete $localFile. Is it still open somewhere?")
    val i = TileverseAccessor.writeTiles(
      dsTile.toLocalIterator().asScala,
      localPath, zoomAndFilter.keys.map(_.toInt).toSeq,
      (x: Double) => logger.info(s"Progress: $x"),
      Map("pps" -> df.schema.filterNot(f => nonFeatureAttributes.contains(f.name))),
      compressTiles
    )

    // copy local file to hadoop if configured
    if (hadoopPath.isDefined) {
      logger.info(s"Copying flatgeobuf file to ${hadoopPath.get}")
      val out = filesystem.create(hadoopPath.get, true)
      val in = Files.newInputStream(localFile.toPath)
      try {
        in.transferTo(out)
      } finally {
        in.close()
        out.close()
      }
    }

    Map("nbOfTiles" -> i)
  }

  val nonFeatureAttributes: Seq[String] = Seq("geometry", "layer", "id") ++ colsToIgnore

  def createMvtTile(compressTiles: Boolean)(features: Seq[Row], z: Int, x: Long, y: Long): Array[Byte] = {
    if (logger.isDebugEnabled()) logger.debug(s"creating tile $z/$x/$y with ${features.size} features")
    val codec = new VectorTileCodec()
    val builder = new VectorTileBuilder()
    val tileDef = PMTilesDataObject.getPmTileMatrix(z).tile(x,y).get()
    val tileBounds = tileDef.extent()
    val extent = 4096.0
    val scaleX = extent / (tileBounds.maxX - tileBounds.minX)
    val scaleY = extent / (tileBounds.maxY - tileBounds.minY)
    val geometryTransform = new AffineTransformation()
      .translate(-tileBounds.minX, -tileBounds.minY)
      .scale(scaleX, scaleY)
    val flipYTransform = new AffineTransformation()
      .scale(1, -1)
      .translate(0, extent)

    features.groupBy(_.getAs[String]("layer")).foreach {
      case (layerName, rows) =>
        val layer = builder.layer().name(layerName)
        val attributes = rows.head.schema.fieldNames
          .diff(nonFeatureAttributes)
        val hasId = rows.head.schema.contains("id")
        rows.foreach {
          row =>
            val geometry = row.getAs[Geometry]("geometry")
            val tileGeometry = flipYTransform.transform(geometryTransform.transform(convert4326to3857(geometry)))
            val feature = layer.feature()
              .geometry(tileGeometry)
            if (hasId) feature.id(row.getAs[Long]("id"))
            row.getValuesMap[Any](attributes)
              .foreach {
                case (k,v) => feature.attribute(k,v)
              }
            feature.build()
        }
        layer.build()
    }

    val tile = builder.build()
    val data = codec.encode(tile)
    if (compressTiles) TileverseAccessor.compressUsingGzip(data) else data
  }

  override def factory: FromConfigFactory[DataObject] = PMTilesDataObject
}

object PMTilesDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): PMTilesDataObject = {
    extract[PMTilesDataObject](config)
  }

  def getPmTileMatrix(zoom: Int): TileMatrix = _pmTileMatrices.getOrElseUpdate(zoom,
    DefaultTileMatrixSets.WORLD_EPSG3857.getTileMatrix(zoom)
  )
  @transient private lazy val _pmTileMatrices = mutable.Map[Int, TileMatrix]()


}

case class TileId(z: Int, x: Long, y: Long) {
  def getIndex: TileIndex = TileIndex.zxy(z, x, y)
}

case class TileData(tile: TileId, data: Array[Byte])