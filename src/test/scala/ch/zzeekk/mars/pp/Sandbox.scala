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

import ch.zzeekk.mars.pp.utils.GeometryCalcUtils
import io.smartdatalake.app.GlobalConfig
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigToolbox, InstanceRegistry}
import io.smartdatalake.lab.{LabSparkDfActionWrapper, LabSparkDfsActionWrapper}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.{CustomDataFrameAction, DataFrameOneToOneActionImpl}
import io.smartdatalake.workflow.dataobject._
import org.apache.sedona.sql.UDT.UdtRegistrator
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.Logger

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object Sandbox extends App with SmartDataLakeLogger {
  val workingDir = System.getProperty("user.dir")
  val env = "dev"
  UdtRegistrator.registerAll()

  private implicit lazy val loggImp: Logger = logger
  private implicit val (registry: InstanceRegistry, globalConfig: GlobalConfig) = ConfigToolbox.loadAndParseConfig(Seq(s"file:///$workingDir/config", s"file:///$workingDir/envConfig/$env.conf"))
  private implicit val spark: SparkSession = globalConfig.sparkSession("test", Some("local[*]"))
  private implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(spark, registry)
  import org.apache.spark.sql.functions._

  // reading a DataFrame from a DataObject
  def dfs[T <: DataObject with CanCreateSparkDataFrame : TypeTag : ClassTag](objectId: String, filter: Column = lit(true)): DataFrame = registry.get[T](DataObjectId(objectId)).getSparkDataFrame().where(filter)

  // Action wrappers (as known from notebook)
  def dfActionWrapper(actionId: String) = LabSparkDfActionWrapper(registry.get[DataFrameOneToOneActionImpl](ActionId(actionId)), context)
  def dfsActionWrapper(actionId: String) = LabSparkDfsActionWrapper(registry.get[CustomDataFrameAction](ActionId(actionId)), context)

  import spark.implicits._
  //dfs[ParquetFileDataObject]("slv-de-isr-streckenabschnitt")
  //  .printSchema()

  //dfs[JsonFileDataObject]("ext-osm-track")
  //  .printSchema()
  def createTagWithPrefixFromNumber(col: Column, prefix: String) = when(col.isNotNull, concat(lit(prefix),col))

  dfs[SparkFileDataObject]("slv-tlm3d-node")
    .where($"uuid_node".isin("b00ad96b-a9d5-4ae4-89d2-4a90bcb83621"))
    .show(false)
  //val df = dfs[DeltaLakeTableDataObject]("slv-pp")
    //.where($"uuid_edge".isin("b4e05052-30de-4d99-9ac1-81d3bb5ca657"))
  //  .where($"uuid_edge".startsWith("5b681701-7769-45e"))
  //df.printSchema()
  //df.groupBy($"objektart").count.show
  //df.orderBy($"uuid_edge", $"edge_idx").show()
  //df.show

  //val udfH3idL15 = udf(PpIdGenerator.getH3idL15 _)
  // getting DataFrames of an Action
  //val dfs = dfsActionWrapper("create-pp").buildDataFrames
  //  .withFilter("uuid_edge", $"uuid_edge".startsWith("5b681701-7769-45e"))
  //  .withFilter("edge_idx", $"edge_idx" <= 3)
  //  .get
  //val dfTlm3d = dfs("slv-tlm3d-pp")
  //val dfPp = dfs("slv-pp-tlm3d")
  //dfPp
    //.withColumn("h3id", udfH3idL15($"x", $"y", lit("EPSG:2056")))
  //  .orderBy($"uuid_edge", $"edge_idx").show

  // 646171828143330547, 646171828143330517, 646171828143330512, 646171828143330513, 646171828143330515

  /*
  // simulating a run
  val sdlb = new DefaultSmartDataLakeBuilder
  val simulationConfig = context.appConfig
    .copy(feedSel = ".*")
  val resultSubFeeds = sdlb.startSimulation(simulationConfig, Seq(), failOnMissingInputSubFeeds = false)
  println(resultSubFeeds._1.map(_.dataObjectId))
  */



}
