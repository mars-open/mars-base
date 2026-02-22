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
package ch.zzeekk.mars.de.isr

import ch.zzeekk.mars.pp.utils.GeometryCalcUtils
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.webservice.SttpUtil.{SttpRequestExtension, createDefaultBackend}
import io.smartdatalake.util.webservice.{HttpProxyConfig, HttpTimeoutConfig, SttpUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataobject.{CanCreateSparkDataFrame, DataObject, DataObjectMetadata}
import org.apache.sedona.common.Functions
import org.apache.sedona.sql.UDT.UdtRegistrator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{explode, from_json}
import org.apache.spark.sql.sedona_sql.expressions.st_constructors.ST_GeomFromGeoJSON
import org.apache.spark.sql.types._
import org.apache.spark.sql.xml.XsdSchemaConverter
import org.locationtech.jts.geom.{Envelope, Geometry}
import sttp.client3.{Identity, SttpBackend, basicRequest}
import sttp.model.Uri

import scala.xml.{Elem, Node, XML}

/**
 * Reads a layer from a WFS (WebFeatureService) GIS Endpoint
 * Cares for cutting layer boundaries into smaller squares, using configurable overlap to avoid loosing features.
 * WFS Service must support output format Json.
 *
 * @param baseUrl Base URL of the WFS endpoint, e.g. http://example.com/geoserver/wfs?service=wfs&version=1.3.0
 *                As a first request the DataObject will call `url&request=getCapabilities` to get selected feature information.
 * @param s2CellLevel Cell level of s2 spatial grid for partitioning requests.
 *                    Default is 10, which gives about 10x10km squares.
 *                    See also https://s2geometry.io/resources/s2cell_statistics.html
 * @param bbox Optional bounding box for retrieving features in EPSG:4326 (WGS84) coordinate reference system.
 *             Default is to retrieve bounding box from WFS FeatureType definition.
 * @param tgtCrs Optional target coordinate reference system passed to the WFS service.
 *               Default is EPSG:4326 (WGS84).
 * @param overlapPct overlap as percentage used when creating smaller square boundaries for WFS queries.
 * @param parallelize Number of tasks to create, that are potentially processed in parallel.
 * @param limit Optional limit of features to read for testing purposes.
 */
case class WfsDataObject(
                          id: DataObjectId,
                          baseUrl: String,
                          feature: String,
                          s2CellLevel: Option[Int] = Some(10),
                          bbox: Option[WGS84BBox] = None,
                          tgtCrs: String = "EPSG:4326",
                          overlapPct: Float = 0.01f,
                          parallelize: Int = 1,
                          limit: Option[Int] = None,
                          proxy: Option[HttpProxyConfig] = None,
                          followRedirects: Boolean = false,
                          retries: Int = 0,
                          urlParameters: Map[String, String] = Map(),
                          additionalHeaders: Map[String, String] = Map(),
                          timeouts: Option[HttpTimeoutConfig] = None,
                          metadata: Option[DataObjectMetadata] = None
                        )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateSparkDataFrame {

  val parsedBaseUrl = Uri.unsafeParse(baseUrl)
  UdtRegistrator.registerAll()

  @transient private lazy implicit val httpBackend: SttpBackend[Identity, Any] = createDefaultBackend(proxy, timeouts)

  def getContent(url: Uri, contentType: String, context: String = "get") = {
    val request = basicRequest
      //.applyAuthMode(authMode)
      .optionalReadTimeout(timeouts)
      .get(url)
      .headers(additionalHeaders)
      .header("Allow", contentType)
      .followRedirects(followRedirects)
    SttpUtil.sendRequest(request, s"($id) $context", retries)
  }

  lazy val capabilities: Elem = {
    logger.info(s"($id) reading capabilities from WFS")
    XML.loadString(
      getContent(parsedBaseUrl.addParam("request", "getCapabilities"), "application/xml", "getCapabilities")
    )
  }

  def getFeatures: Seq[String] = {
    (capabilities \\ "FeatureType" \\ "Name").map(_.text.trim).filter(_.nonEmpty)
  }

  def getFeatureBBox: WGS84BBox = {
    val featureDef = (capabilities \\ "FeatureType")
      .find(ft => (ft \\ "Name").text.trim == feature)
    featureDef.flatMap(WGS84BBox.fromFeatureDef)
      .getOrElse(throw new RuntimeException(s"($id) Could not find bounding box for feature $feature in $featureDef"))
  }

  lazy val jsonSchema: StructType = {
    logger.info(s"($id) Reading from WFS using describeFeatureType for feature $feature")
    val xsdString = getContent(parsedBaseUrl.addParam("request", "describeFeatureType")
      .addParam("typeName", feature), "application/json", "describeFeatureType")
    val properties = XsdSchemaConverter.read(xsdString, maxRecursion = 10, prefixesToIgnore = Seq("gml")) // no gml info in json
      .head.dataType.asInstanceOf[StructType] // skip wrapper element
    StructType(Seq(
      StructField("type", StringType),
      StructField("features", ArrayType(StructType(Seq(
        StructField("type", StringType),
        StructField("id", StringType),
        StructField("geometry", StringType),
        StructField("properties", properties)
      ))))
    ))
  }

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = {
    val session = context.sparkSession
    import session.implicits._
    assert(getFeatures.contains(feature), s"($id) Feature $feature not existing in WFS Service. Available features are ${getFeatures.mkString(", ")}")

    // prepare partition geometries
    val bboxGeom = GeometryCalcUtils.getGeoFactory("EPSG:4326").toGeometry(bbox.getOrElse(getFeatureBBox).getEnvelope)
    val partitionGeoms = s2CellLevel
      .map { l =>
        val cellIds = Functions.s2CellIDs(bboxGeom, l).map(_.toLong)
        val geoms = Functions.s2ToGeom(cellIds)
        cellIds.zip(geoms).map{ case (s2id, geom) => WfsRequest(Some(s2id), geom)}.toSeq
      }
      .getOrElse(Seq(WfsRequest(None, bboxGeom)))
    val partitionGeomsLimited = if (context.isExecPhase) {
      logger.info(s"($id) Reading WFS feature $feature using ${partitionGeoms.size} partitions")
      limit.map(nb => partitionGeoms.take(nb)).getOrElse(partitionGeoms)
    } else Seq()

    // read from wfs
    val dfResult = session.createDataset(partitionGeomsLimited)
      .repartition(parallelize)
      .map{ req =>
        val url = parsedBaseUrl
          .addParam("request", "getFeature")
          .addParam("typeNames", feature)
          .addParam("srsName", tgtCrs)
          .addParam("bbox", req.getBboxParam(overlapPct, tgtCrs)) // specify CRS for WFS 1.1.0 and higher
          .addParam("outputFormat", "application/json")
        val json = getContent(url, "application/json", s"getFeature s2id=${req.s2id.getOrElse("full")}")
        WfsResponse(req.s2id, json)
      }
      .withColumn("feature", explode(from_json($"json", jsonSchema)("features")))
      .select(
        $"s2id",
        $"feature.id".as("feature_id"),
        ST_GeomFromGeoJSON("feature.geometry").as("geometry"),
        $"feature.properties.*"
      )
      .toDF()
    //if (!context.isExecPhase) dfResult.printSchema
    //if (context.isExecPhase) dfResult.cache.show
    dfResult
  }

  override def factory: FromConfigFactory[DataObject] = WfsDataObject
}

object WfsDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): WfsDataObject = {
    extract[WfsDataObject](config)
  }
}


case class WfsRequest(s2id: Option[Long], geometry: Geometry) {
  def getBboxParam(overlap: Double, crs: String): String = {
    val transformed = GeometryCalcUtils.convertCrs(geometry, "EPSG:4326", crs)
    val env = transformed.getEnvelopeInternal
    val minX = env.getMinX - env.getWidth * overlap
    val maxX = env.getMaxX + env.getWidth * overlap
    val minY = env.getMinY - env.getHeight * overlap
    val maxY = env.getMaxY + env.getHeight * overlap
    s"$minX,$minY,$maxX,$maxY,$crs"
  }
}

case class WfsResponse(s2id: Option[Long], json: String)

case class WGS84BBox(minX: Double, minY: Double, maxX: Double, maxY: Double) {
  //validateWGS84Coordinates
  require(minX >= -180 && minX <= 180, s"Invalid minX: $minX. Longitude must be between -180 and 180.")
  require(maxX >= -180 && maxX <= 180, s"Invalid maxX: $maxX. Longitude must be between -180 and 180.")
  require(minY >= -90 && minY <= 90, s"Invalid minY: $minY. Latitude must be between -90 and 90.")
  require(maxY >= -90 && maxY <= 90, s"Invalid maxY: $maxY. Latitude must be between -90 and 90.")
  require(minX < maxX, s"minX ($minX) must be less than maxX ($maxX).")
  require(minY < maxY, s"minY ($minY) must be less than maxY ($maxY).")

  def getEnvelope: Envelope = new Envelope(minX, maxX, minY, maxY)
}
object WGS84BBox {
  /**
   * Parse the bounding box of the feature from the capabilities XML.
   * Possible formats:
   * <LatLongBoundingBox minx="6.5" miny="45.8" maxx="10.5" maxy="47.8"/>
   * or
   * <ows:WGS84BoundingBox>
   * <ows:LowerCorner>6.0547424293034195 47.494262115424085</ows:LowerCorner>
   * <ows:UpperCorner>14.9822210089651 54.78506885601786</ows:UpperCorner>
   * </ows:WGS84BoundingBox>
   * @return Bounding box of the feature in WGS84 coordinates
   */
  def fromFeatureDef(featureDef: Node): Option[WGS84BBox] = {
    (featureDef \\ "LatLongBoundingBox").headOption.map { bbox =>
      WGS84BBox((bbox \ "@minx").text.toDouble, (bbox \ "@miny").text.toDouble, (bbox \ "@maxx").text.toDouble, (bbox \ "@maxy").text.toDouble)
    }.orElse {
      val lowerCorner = (featureDef \\ "WGS84BoundingBox" \\ "LowerCorner").headOption.map(_.text.trim)
      val upperCorner = (featureDef \\ "WGS84BoundingBox" \\ "UpperCorner").headOption.map(_.text.trim)
      for {
        lower <- lowerCorner; upper <- upperCorner
        Array(minX, minY) = lower.split(" ").filter(_.nonEmpty).map(_.toDouble)
        Array(maxX, maxY) = upper.split(" ").filter(_.nonEmpty).map(_.toDouble)
      } yield WGS84BBox(minX, minY, maxX, maxY)
    }
  }
}