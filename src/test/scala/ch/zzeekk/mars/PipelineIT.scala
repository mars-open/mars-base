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
package ch.zzeekk.mars

import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, SmartDataLakeBuilderConfig, TestMode}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

/**
 * These tests are integration tests of the data pipeline.
 * They work with the DataObject schemas exported to ./schema directory on production system
 */
class PipelineIT extends SmartDataLakeLogger with AnyFlatSpecLike with Matchers {
  private val workingDir = System.getProperty("user.dir")

  "dry-run" should "validate pipeline" in {
    val feedSel = "ids:create-slv-tlm3d-node"
    val envConfig = sys.env.getOrElse("ENV_CONFIG", "local.conf")
    logger.info(s"START dry-run: feedSel = $feedSel , envConfig = $envConfig")
    val config = SmartDataLakeBuilderConfig(feedSel,
      applicationName = Some("debug"), master = Some("local[2]"), deployMode = Some("client"), test = Some(TestMode.DryRun),
      configuration = Seq(s"file:///$workingDir/config")) //, s"file:///$workingDir/envConfig/$envConfig"))
    val sdlb = DefaultSmartDataLakeBuilder
    sdlb.run(config)
  }
}
