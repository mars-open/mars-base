package ch.zzeekk.mars.pp.utils

import io.smartdatalake.app.SDLPlugin
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.sedona.sql.UDT.UdtRegistrator

class SedonaInitPlugin extends SDLPlugin with SmartDataLakeLogger {

  override def startup(): Unit = {
    UdtRegistrator.registerAll()
    logger.info("SedonaInitPlugin startup finished")
  }

}
