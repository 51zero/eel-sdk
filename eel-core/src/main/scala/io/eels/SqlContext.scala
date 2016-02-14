package io.eels

import java.util.UUID

import io.eels.component.jdbc.{JdbcSinkProps, JdbcSink, JdbcSource}

class SqlContext {

  Class.forName("org.h2.Driver")
  val uri = s"jdbc:h2:mem:sqlcontext${UUID.randomUUID.toString.replace("-", "")};IGNORECASE=TRUE;DB_CLOSE_DELAY=-1"

  def registerFrame(name: String, frame: Frame): Unit = {
    frame.to(JdbcSink(uri, name, JdbcSinkProps(createTable = true))).run
  }

  def sql(query: String): Frame = JdbcSource(uri, query)
}

object SqlContext {
  def apply(): SqlContext = new SqlContext
}