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
