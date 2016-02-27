package io.eels

import java.util.UUID

import com.typesafe.config.ConfigFactory
import io.eels.component.jdbc.{JdbcSink, JdbcSinkProps, JdbcSource}

import scala.concurrent.ExecutionContext

class SqlContext {
  Class.forName("org.h2.Driver")

  val config = ConfigFactory.load()
  val disk = config.getBoolean("eel.sqlContext.writeToDisk")
  val ignoreCase = config.getBoolean("eel.sqlContext.ignoreCase").toString.toUpperCase

  val uri = if (disk) {
    s"jdbc:h2:sqlcontext${UUID.randomUUID.toString.replace("-", "")};IGNORECASE=$ignoreCase;DB_CLOSE_DELAY=-1"
  } else {
    s"jdbc:h2:mem:sqlcontext${UUID.randomUUID.toString.replace("-", "")};IGNORECASE=$ignoreCase;DB_CLOSE_DELAY=-1"
  }

  def registerFrame(name: String, frame: Frame)(implicit executionContext: ExecutionContext): Unit = {
    frame.to(JdbcSink(uri, name, JdbcSinkProps(createTable = true)))
  }

  def sql(query: String): Frame = JdbcSource(uri, query)
}

object SqlContext {
  def apply(): SqlContext = new SqlContext
}
