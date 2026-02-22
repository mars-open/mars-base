package ch.zzeekk.mars.pp

import ch.zzeekk.mars.pp.utils.SparkUtils.jsonifyComplexDataTypes
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.dataobject.{CanWriteSparkDataFrame, DataObject, DataObjectMetadata}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.geotools.api.data.SimpleFeatureStore
import org.geotools.data.DefaultTransaction
import org.geotools.data.flatgeobuf.FlatGeobufDataStoreFactory
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.geotools.referencing.crs.DefaultGeographicCRS

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IteratorHasAsScala, MapHasAsJava}

/**
 * Creates a FlatGeobuf file containing the geometries of the DataFrame.
 * Each record is treated as feature and needs a geometry column.
 * The DataFrame
 * - needs a column with name "geometry", having a Sedona Geometry DataType
 * - can have a column with name "id" with type "long", used as feature id
 *
 * Note that geometries are expected in WGS84 / EPSG:4326 coordinate reference system.
 *
 * Sometimes the resulting file is corrupt when reading in JS. Error is "start offset of Float64Array should be a multiple of 8".
 * When removing uuid Columns it works again...
 * A workaround is to enable ogr2ogr rewrite.
 * Make sure that ogr2ogr binary is resolvable through PATH environment variable.
 */
case class FlatGeobufDataObject(
                                 id: DataObjectId,
                                 localPath: String,
                                 path: Option[String] = None,
                                 geometryType: String = "LineString",
                                 geometryColName: String = "geometry",
                                 idColName: Option[String] = None,
                                 colsToIgnore: Seq[String] = Seq(),
                                 ogr2OgrFix: Boolean = false,
                                 metadata: Option[DataObjectMetadata] = None
                               )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanWriteSparkDataFrame {

  val geometryClassName: String = if (geometryType.contains(".")) geometryType else s"org.locationtech.jts.geom.$geometryType"
  val geometryClass: Class[_] = getClass.getClassLoader.loadClass(geometryClassName)

  assert(localPath.endsWith(".fgb"), "Local path needs to end with '.fgb'!")
  assert(path.forall(p => p.endsWith(".fgb")), "Path needs to end with '.fgb'")
  val hadoopPath: Option[Path] = path.map(HdfsUtil.prefixHadoopPath(_, None))
  def filesystem(implicit context: ActionPipelineContext): FileSystem = {
    if (filesystemHolder == null && hadoopPath.isDefined) {
      filesystemHolder = HdfsUtil.getHadoopFsWithConf(hadoopPath.get)(context.hadoopConf)
    }
    filesystemHolder
  }
  @transient private var filesystemHolder: FileSystem = _

  val nonFeatureAttributes: Set[String] = Set(geometryColName) ++ idColName.toSet ++ colsToIgnore
  val featureName: String = localPath.stripSuffix(".fgb").split("[/\\\\]").last

  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap = {
    implicit val session: SparkSession = df.sparkSession

    assert(df.columns.contains(geometryColName))

    val dfSimplified = jsonifyComplexDataTypes(df)

    // Build FeatureType from schema
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.setName(featureName)
    sftBuilder.setCRS(DefaultGeographicCRS.WGS84)
    //def replaceM1(n: Int) = if (n == -1) 99999 else n
    dfSimplified.schema
      .filterNot(f => nonFeatureAttributes.contains(f.name))
      //.sortWith{case (a,b) => replaceM1(colOrder.indexOf(a.name)) < replaceM1(colOrder.indexOf(b.name)) }
      .foreach { field =>
        field.dataType match {
          case StringType if field.name.startsWith("uuid") => sftBuilder.add(field.name, classOf[String])
          case StringType => sftBuilder.add(field.name, classOf[String])
          case ShortType => sftBuilder.add(field.name, classOf[java.lang.Short])
          case IntegerType => sftBuilder.add(field.name, classOf[java.lang.Integer])
          case LongType => sftBuilder.add(field.name, classOf[java.lang.Long])
          case DoubleType => sftBuilder.add(field.name, classOf[java.lang.Double])
          case FloatType => sftBuilder.add(field.name, classOf[java.lang.Float])
          case BooleanType => sftBuilder.add(field.name, classOf[java.lang.Boolean])
          case TimestampType => sftBuilder.add(field.name, classOf[java.sql.Timestamp])
          case DateType => sftBuilder.add(field.name, classOf[java.sql.Date])
          case _ => sftBuilder.add(field.name, classOf[String]) // Fallback
        }
    }
    sftBuilder.add(geometryColName, geometryClass)
    sftBuilder.setDefaultGeometry(geometryColName)
    val featureType = sftBuilder.buildFeatureType()
    val featureAttributes = featureType.getTypes.asScala.map(_.getName.toString).toSeq

    // Collect to FeatureCollection
    val featureCollection = new DefaultFeatureCollection()
    dfSimplified.toLocalIterator().asScala
      .zipWithIndex.foreach {
        case (row, idx) =>
          val builder = new SimpleFeatureBuilder(featureType)
          featureAttributes.foreach(a => builder.add(row.getAs[Any](a)))
          val id = if (idColName.isDefined) row.getAs[Any](idColName.get) else idx
          val feature = builder.buildFeature(id.toString)
          featureCollection.add(feature)
      }

    // write to local file
    val localFile = new File(localPath)
    localFile.delete()
    assert(!localFile.exists(), s"Could not delete $localFile. Is it still open somewhere?")
    Files.createDirectories(localFile.toPath.getParent)
    val localFileURL = localFile.getAbsoluteFile.getCanonicalFile.toURI.toURL
    logger.info(s"Creating local flatgeobuf file at $localFileURL")
    val factory = new FlatGeobufDataStoreFactory()
    val dataStore = factory.createDataStore(Map("url" -> localFileURL).asJava)
    dataStore.createSchema(featureType)
    val featureStore = dataStore.getFeatureSource(featureName).asInstanceOf[SimpleFeatureStore]
    val transaction = new DefaultTransaction(s"$id#${context.executionId.runId}")
    featureStore.setTransaction(transaction)
    featureStore.addFeatures(featureCollection)
    transaction.commit()
    transaction.close()
    dataStore.dispose()

    // fix with ogr2ogr workaround if configured
    if (ogr2OgrFix) {
      val localFileOrg = new File(localFile.toString.stripSuffix(".fgb") + "-org.fgb")
      Files.move(localFile.toPath, localFileOrg.toPath, StandardCopyOption.REPLACE_EXISTING)
      val cmd = Seq("ogr2ogr", "-f", "FlatGeobuf", localFile.toString, localFileOrg.toString, "-skipfailures", "-lco", "SPATIAL_INDEX=YES")
      import scala.sys.process._
      if (cmd.! == 0) logger.info("OGR has rewritten FGB-File")
      else throw new RuntimeException("OGR failed rewriting FGB-File")
    }

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

    Map("nbOfGeometries" -> featureCollection.size())
  }

  override def factory: FromConfigFactory[DataObject] = FlatGeobufDataObject
}

object FlatGeobufDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): FlatGeobufDataObject = {
    extract[FlatGeobufDataObject](config)
  }
}