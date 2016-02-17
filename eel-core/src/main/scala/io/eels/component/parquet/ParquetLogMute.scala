package io.eels.component.parquet

import java.util.logging.Logger

import scala.util.Try

object ParquetLogMute {

  // the handler is in a static init block so we must force loading of the class before we unapply it, otherwise
  // when the class is later on loaded, it will just add it back
  def apply(): Unit = {
    Try { Class.forName("org.apache.parquet.Log") }
    Try { Class.forName("parquet.Log") }
    for ( pack <- Seq("org.apache.parquet", "parquet") ) {
      Try {
        val logger = Logger.getLogger(pack)
        logger.getHandlers.foreach(logger.removeHandler)
        logger.setUseParentHandlers(false)
      }
    }
  }
}
