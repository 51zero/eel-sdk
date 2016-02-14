package io.eels.component.parquet

import java.util.logging.Logger

import scala.util.Try

object ParquetLogMute {

  val packages = Seq("org.apache.parquet", "parquet")

  def apply(): Unit = {
    for ( pack <- packages ) {
      Try {
        val logger = Logger.getLogger(pack)
        logger.getHandlers.foreach(logger.removeHandler)
        logger.setUseParentHandlers(false)
      }
    }
  }
}
