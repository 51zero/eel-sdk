package io.eels

import java.util.UUID

import com.typesafe.config.ConfigFactory
import io.eels.component.jdbc.JdbcSink
import io.eels.component.jdbc.JdbcSinkProps
import io.eels.component.jdbc.JdbcSource

class SqlContext {
  init {
    Class.forName("org.h2.Driver")
  }

  val config = ConfigFactory.load()
  val disk = config.getBoolean("eel.sqlContext.writeToDisk")
  val dataDirectory = config.getString("eel.sqlContext.dataDirectory")
  val ignoreCase = config.getBoolean("eel.sqlContext.ignoreCase").toString().toUpperCase()

  val uri = if (disk) {
    "jdbc:h2:$dataDirectory/sqlcontext${UUID.randomUUID().toString().replace("-", "")};IGNORECASE=$ignoreCase;DB_CLOSE_DELAY=-1"
  } else {
    "jdbc:h2:mem:sqlcontext${UUID.randomUUID().toString().replace("-", "")};IGNORECASE=$ignoreCase;DB_CLOSE_DELAY=-1"
  }

  fun registerFrame(name: String, frame: Frame): Unit {
    frame.to(JdbcSink(uri, name, JdbcSinkProps(createTable = true)))
  }

  fun sql(query: String): Frame = JdbcSource(uri, query).toFrame(1)
}