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

import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, SmartDataLakeBuilderConfig}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.io.File

/**
 * Integration test for incremental loads of Positionpoints:
 * running create-slv-ch-pp again on unchanged slv-tlm3d-pp must not create new Positionpoints.
 *
 * To keep it fast, only tlm3d tracks in a 50km square around Zürich are processed,
 * and data is written to a separate directory ./data/it (see configurationValueOverwrite in runSdlb).
 * Input data is read from ./data/ext/tlm3d.
 * ENV_CONFIG selects the base environment: a file name in ./envConfig (default dev.conf) or a path.
 */
class IncrementalPipelineIT extends SmartDataLakeLogger with AnyFlatSpecLike with Matchers with BeforeAndAfterAll {
  private val workingDir = System.getProperty("user.dir")
  private val envConfig = sys.env.getOrElse("ENV_CONFIG", "dev.conf")
  private val envConfigPath = if (envConfig.contains("/")) envConfig else s"$workingDir/envConfig/$envConfig"
  private val dataPath = s"$workingDir/data/it"
  private val slvPpPath = s"$dataPath/slv/pp"
  private val slvPpMappingPath = s"$dataPath/slv/ch_pp_mapping_tlm3d"

  private def runSdlb(feedSel: String): Unit = {
    val config = SmartDataLakeBuilderConfig(feedSel, applicationName = Some("incremental-it"),
      configuration = Seq(s"file:///$workingDir/config", s"file:///$envConfigPath"),
      configurationValueOverwrite = Map(
        "env.dataPath" -> "./data/it",
        "env.tlm3dTrackFilter" -> "ST_Intersects(geometry, ST_PolygonFromEnvelope(2658000, 1223000, 2708000, 1273000))"
      )
    )
    DefaultSmartDataLakeBuilder.run(config)
  }

  /**
   * Spark session shared with SDLB
   */
  private lazy val session: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("incremental-it")
    .config("spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions,io.delta.sql.DeltaSparkSessionExtension")
    .getOrCreate()

  override def beforeAll(): Unit = session

  override def afterAll(): Unit = session.stop()

  private def countPps(): Long = session.read.format("delta").load(slvPpPath).count()

  "create-slv-ch-pp" should "not create new Positionpoints when repeated" in {
    FileUtils.deleteDirectory(new File(dataPath))

    // prepare edges and Positionpoint candidates for the region
    runSdlb("tlm3d-ext,tlm3d")

    runSdlb("ids:create-slv-ch-pp")
    val cnt1 = countPps()
    logger.info(s"slv-pp count after first run: $cnt1")
    cnt1 should be > 0L

    runSdlb("ids:create-slv-ch-pp")
    val cnt2 = countPps()
    logger.info(s"slv-pp count after second run: $cnt2")
    cnt2 shouldBe cnt1

    // integrity: unique ids, mapping references existing Positionpoints only
    val pps = session.read.format("delta").load(slvPpPath)
    pps.groupBy("id_positionpoint").count().where(col("count") > 1).count() shouldBe 0
    val mapping = session.read.format("delta").load(slvPpMappingPath)
    mapping.join(pps, Seq("id_positionpoint"), "left_anti").count() shouldBe 0
  }
}
