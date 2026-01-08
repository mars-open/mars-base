package ch.zzeekk.mars

import io.smartdatalake.app.GlobalConfig
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigToolbox, InstanceRegistry}
import io.smartdatalake.lab.{LabSparkDfActionWrapper, LabSparkDfsActionWrapper}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.{CustomDataFrameAction, DataFrameOneToOneActionImpl}
import io.smartdatalake.workflow.dataobject.{CanCreateSparkDataFrame, DataObject, SparkFileDataObject}
import org.apache.sedona.sql.UDT.UdtRegistrator
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.Logger

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object Sandbox extends App with SmartDataLakeLogger {
  val workingDir = System.getProperty("user.dir")
  UdtRegistrator.registerAll()

  private implicit lazy val loggImp: Logger = logger
  private implicit val (registry: InstanceRegistry, globalConfig: GlobalConfig) = ConfigToolbox.loadAndParseConfig(Seq(s"file:///$workingDir/config"))
  private implicit val spark: SparkSession = globalConfig.sparkSession("test", Some("local[2]"))
  private implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(spark, registry)
  import spark.implicits._
  import org.apache.spark.sql.functions._

  // reading a DataFrame from a DataObject
  def dfs[T <: DataObject with CanCreateSparkDataFrame : TypeTag : ClassTag](objectId: String, filter: Column = lit(true)): DataFrame = registry.get[T](DataObjectId(objectId)).getSparkDataFrame().where(filter)

  // Action wrappers (as known from notebook)
  def dfActionWrapper(actionId: String) = LabSparkDfActionWrapper(registry.get[DataFrameOneToOneActionImpl](ActionId(actionId)), context)
  def dfsActionWrapper(actionId: String) = LabSparkDfsActionWrapper(registry.get[CustomDataFrameAction](ActionId(actionId)), context)

  val df = dfs[SparkFileDataObject]("slv-tlm3d-edge")
    .where($"uuid_edge".isin("b4e05052-30de-4d99-9ac1-81d3bb5ca657"))
  df.printSchema()
  //df.groupBy($"objektart").count.show
  df.show(100, false)

  // getting DataFrames of an Action
  //val dfs = dfsActionWrapper("create-slv-tlm3d-pp").buildDataFrames
  //  .withFilter("uuid_edge", $"uuid_edge"==="bd31e7c5-7ba3-4bb2-b6a3-4369c02f0222").get
  //val dfPp = dfs("slv-tlm3d-pp")
  //dfPp.orderBy($"position").show

  /*
  // simulating a run
  val sdlb = new DefaultSmartDataLakeBuilder
  val simulationConfig = context.appConfig
    .copy(feedSel = ".*")
  val resultSubFeeds = sdlb.startSimulation(simulationConfig, Seq(), failOnMissingInputSubFeeds = false)
  println(resultSubFeeds._1.map(_.dataObjectId))
  */



}
