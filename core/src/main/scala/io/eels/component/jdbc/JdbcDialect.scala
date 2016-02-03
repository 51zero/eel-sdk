package io.eels.component.jdbc

import io.eels.{Column, Row, FrameSchema, SchemaType}

trait JdbcDialect {
  def create(schema: FrameSchema, table: String): String
  def insert(row: Row, table: String): String
}

object JdbcDialect {

  /**
    * Detect dialect from the connection string
    */
  def apply(url: String): JdbcDialect = GenericJdbcDialect
}

object GenericJdbcDialect extends JdbcDialect {

  def toTypeString(column: Column): String = column.`type` match {
    case SchemaType.Int => "int"
    case SchemaType.Short => "smallint"
    case _ => s"varchar(${column.precision})"
  }

  override def create(schema: FrameSchema, table: String): String = {
    val columns = schema.columns.map(c => s"${c.name} ${toTypeString(c)}").mkString("(", ",", ")")
    s"CREATE TABLE $table $columns"
  }

  override def insert(row: Row, table: String): String = {
    val columns = row.columns.map(_.name).mkString(",")
    val values = row.fields.map(_.value).mkString("'", "','", "'")
    s"INSERT INTO $table ($columns) VALUES ($values)"
  }
}