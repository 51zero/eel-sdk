package io.eels.component.jdbc

import io.eels.{Row, FrameSchema, SchemaType}

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

  def toTypeString(schemaType: SchemaType): String = schemaType match {
    case SchemaType.String => "varchar(255)"
    case SchemaType.Int => "int"
    case _ => "varchar(255)"
  }

  override def create(schema: FrameSchema, table: String): String = {
    val columns = schema.columns.map(c => s"${c.name} ${toTypeString(c.`type`)}").mkString("(", ",", ")")
    s"CREATE TABLE $table $columns"
  }

  override def insert(row: Row, table: String): String = {
    val columns = row.columns.map(_.name).mkString(",")
    val values = row.fields.map(_.value).mkString("'", "','", "'")
    s"INSERT INTO $table ($columns) VALUES ($values)"
  }
}