package com.sksamuel.hs.sink

import java.sql.{DriverManager, ResultSet}

import com.sksamuel.hs.Sink
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.language.implicitConversions

case class JdbcSink(url: String, table: String, props: JdbcSinkProps = JdbcSinkProps())
  extends Sink
    with StrictLogging {

  private lazy val conn = DriverManager.getConnection(url)

  lazy val tables = ResultsetIterator(conn.getMetaData.getTables(null, null, null, null)).map(_.apply(3).toLowerCase)
  private var created = false

  def createTable(row: Row): Unit = {
    if (!created && props.createTable && !tables.contains(table.toLowerCase)) {
      val columns = row.columns.map(c => s"${c.name} VARCHAR").mkString("(", ",", ")")
      val stmt = s"CREATE TABLE $table $columns"
      logger.debug("Creating table [$stmt]")
      conn.createStatement().executeUpdate(stmt)
    }
    created = true
  }

  override def completed(): Unit = conn.close()

  override def insert(row: Row): Unit = {
    createTable(row)
    val columns = row.columns.map(_.name).mkString(",")
    val values = row.fields.map(_.value).mkString("'", "','", "'")
    val stmt = s"INSERT INTO $table ($columns) VALUES ($values)"
    conn.createStatement().executeUpdate(stmt)
  }
}

case class JdbcSinkProps(createTable: Boolean = false)

object ResultsetIterator {
  def apply(rs: ResultSet): Iterator[Array[String]] = new Iterator[Array[String]] {
    override def hasNext: Boolean = rs.next()
    override def next(): Array[String] = {
      for ( k <- 1 to rs.getMetaData.getColumnCount ) yield rs.getString(k)
    }.toArray
  }
}

case class Column(name: String)

object Column {
  implicit def toField(str: String): Column = Column(str)
}

case class Field(value: String)

object Field {
  implicit def toField(str: String): Field = Field(str)
}

case class Row(columns: Seq[Column], fields: Seq[Field]) {
  require(columns.size == fields.size, "Columns and fields should have the same size")

  def size: Int = columns.size

  def addColumn(name: String, value: String): Row = {
    copy(columns = columns :+ Column(name), fields = fields :+ Field(value))
  }
}