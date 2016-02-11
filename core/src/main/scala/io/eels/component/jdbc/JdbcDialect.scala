package io.eels.component.jdbc

import com.typesafe.scalalogging.slf4j.StrictLogging
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

object GenericJdbcDialect extends JdbcDialect with StrictLogging {

  def toTypeString(column: Column): String = column.`type` match {
    case SchemaType.Long => "int"
    case SchemaType.BigInt => "int"
    case SchemaType.Int => "int"
    case SchemaType.Short => "smallint"
    case SchemaType.String if column.precision > 0 => s"varchar(${column.precision})"
    case SchemaType.String => "varchar(255)"
    case other =>
      logger.warn(s"Unknown schema type $other")
      "varchar(255)"
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