package io.eels.component.parquet

import java.util.logging.Logger

object ParquetLogMute {

  fun apply() {

    try {
      Class.forName("org.apache.parquet.Log")
    } catch(e: Exception) {
    }

    try {
      Class.forName("parquet.Log")
    } catch(e: Exception) {
    }

    for (pack in listOf("org.apache.parquet", "parquet")) {
      try {
        val logger = Logger.getLogger(pack)
        logger.handlers.forEach { logger.removeHandler(it) }
        logger.useParentHandlers = false
      } catch(e: Exception) {
      }
    }
  }
}