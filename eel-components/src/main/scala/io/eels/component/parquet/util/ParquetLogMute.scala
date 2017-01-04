package io.eels.component.parquet.util

import java.util.logging.Logger

import scala.util.Try

object ParquetLogMute {

  def apply(): Unit = {
    Try {
      Class.forName("org.apache.parquet.Log")
    }

    Try {
      Class.forName("parquet.Log")
    }

    for (pack <- List("org.apache.parquet", "parquet")) {
      Try {
        val logger = Logger.getLogger(pack)
        logger.getHandlers.foreach(logger.removeHandler)
        logger.setUseParentHandlers(false)
      }
    }

    val rootLogger = Logger.getLogger("")
    rootLogger.getHandlers.foreach(rootLogger.removeHandler)
  }
}