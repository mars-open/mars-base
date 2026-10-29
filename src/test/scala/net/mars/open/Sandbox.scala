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
package net.mars.open

import io.smartdatalake.app.GlobalConfig
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigToolbox, InstanceRegistry}
import io.smartdatalake.lab.{LabSparkDfActionWrapper, LabSparkDfsActionWrapper}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.{CustomDataFrameAction, DataFrameOneToOneActionImpl}
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, TransactionalTableDataObject}
import io.smartdatalake.workflow.dataobject.spark.CanCreateSparkDataFrame
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
  private implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(registry)
  private implicit val spark: SparkSession = SparkSubFeed.getSparkSession(context)
  import org.apache.spark.sql.functions._

  // reading a DataFrame from a DataObject
  def dfs(objectId: String, filter: Column = lit(true)): DataFrame = registry.get[DataObject with CanCreateDataFrame](DataObjectId(objectId)).getDataFrame()
    .asInstanceOf[SparkDataFrame].inner.where(filter)

  // Action wrappers (as known from notebook)
  def dfActionWrapper(actionId: String) = LabSparkDfActionWrapper(registry.get[DataFrameOneToOneActionImpl](ActionId(actionId)), context)
  def dfsActionWrapper(actionId: String) = LabSparkDfsActionWrapper(registry.get[CustomDataFrameAction](ActionId(actionId)), context)

  import spark.implicits._
  //dfs("slv-de-isr-streckenabschnitt")
  //  .printSchema()

  //val df = dfs("slv-osm-edge")
  //  .where($"uuid_edge"==="468d0e61-29b9-3329-a9af-7bb0e5029814")

  //val df = dfs("slv-osm-switch")
  //  .where($"id".isin(3893521084L, 3893521085L, 3893520406L, 3893521088L, 3893521086L, 3893521082L, 3893521078L, 3893521076L, 13322955917L, 3893520746L, 13322955916L, 3893520741L))
  //dfs("slv-tlm3d-node")
  //  .where($"uuid_node".isin("b00ad96b-a9d5-4ae4-89d2-4a90bcb83621"))
  //val df = dfs("slv-osm-edge")
    //.where($"tracks.uuid_track"(0).like("%-0000-000017020338")) // -> 468d0e61-29b9-3329-a9af-7bb0e5029814, 8291331f-e2ba-30f6-9d1d-1cc4cd219e46
  //  .where($"src_id_node_to" === "3893521078") // -> 37a99c53-3ebf-3e82-a94b-bae82b6e2bc3
  //  .where($"uuid_node".isin("b00ad96b-a9d5-4ae4-89d2-4a90bcb83621"))
  //val dfTrack = dfs("slv-osm-track")
  //  .where($"uuid".like("%-0000-000017020338"))
  val df = dfs("slv-de-pp-mapping-osm")
  df.printSchema()
  df.show(10, false)
  //print("Count="+df.count)
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
