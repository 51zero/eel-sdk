package io.eels.component.jdbc

import java.sql.DriverManager

import com.sksamuel.scalax.jdbc.ResultSetIterator
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Row, Sink, Writer}

import scala.language.implicitConversions

case class JdbcSink(url: String, table: String, props: JdbcSinkProps = JdbcSinkProps())
  extends Sink
    with StrictLogging {

  override def writer: Writer = new Writer {

    logger.debug(s"Connecting to jdbc sink $url...")
    val conn = DriverManager.getConnection(url)
    logger.debug(s"Connected to $url")

    lazy val tableExists: Boolean = {
      logger.debug("Fetching tables to detect if table exists")
      val tables = ResultSetIterator(conn.getMetaData.getTables(null, null, null, Array("TABLE"))).toList
      logger.debug(s"${tables.size} tables found")
      tables.map(_.apply(3).toLowerCase) contains table.toLowerCase
    }

    var created = false

    def createTable(row: Row): Unit = {
      if (props.createTable && !created && !tableExists) {

        val columns = row.columns.map(c => s"${c.name} VARCHAR(255)").mkString("(", ",", ")")
        val sql = s"CREATE TABLE $table $columns"
        logger.debug(s"Creating table [$sql]")

        val stmt = conn.createStatement()
        try {
          stmt.executeUpdate(sql)
        } finally {
          stmt.close()
        }
      }
      created = true
    }

    override def close(): Unit = conn.close()

    override def write(row: Row): Unit = {
      createTable(row)

      val columns = row.columns.map(_.name).mkString(",")
      val values = row.fields.map(_.value).mkString("'", "','", "'")
      val sql = s"INSERT INTO $table ($columns) VALUES ($values)"
      logger.debug(s"Inserting [$sql]")

      val stmt = conn.createStatement()
      try {
        stmt.executeUpdate(sql)
      } finally {
        stmt.close()
      }
    }
  }
}

case class JdbcSinkProps(createTable: Boolean = false)

