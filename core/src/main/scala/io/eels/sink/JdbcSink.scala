package io.eels.sink

import java.sql.DriverManager

import com.sksamuel.scalax.jdbc.ResultSetIterator
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Writer, Sink, Row}

import scala.language.implicitConversions

case class JdbcSink(url: String, table: String, props: JdbcSinkProps = JdbcSinkProps())
  extends Sink
    with StrictLogging {

  override def writer: Writer = new Writer {

    val conn = DriverManager.getConnection(url)
    val tables = ResultSetIterator(conn.getMetaData.getTables(null, null, null, Array("TABLE"))).map(_.apply(3).toLowerCase)
    var created = false

    def createTable(row: Row): Unit = {
      if (!created && props.createTable && !tables.contains(table.toLowerCase)) {
        val columns = row.columns.map(c => s"${c.name} VARCHAR").mkString("(", ",", ")")
        val stmt = s"CREATE TABLE $table $columns"
        logger.debug(s"Creating table [$stmt]")
        conn.createStatement().executeUpdate(stmt)
      }
      created = true
    }

    override def close(): Unit = conn.close()

    override def write(row: Row): Unit = {
      createTable(row)
      val columns = row.columns.map(_.name).mkString(",")
      val values = row.fields.map(_.value).mkString("'", "','", "'")
      val stmt = s"INSERT INTO $table ($columns) VALUES ($values)"
      conn.createStatement().executeUpdate(stmt)
    }
  }
}

case class JdbcSinkProps(createTable: Boolean = false)

