package io.eels.component.jdbc.dialect

import java.sql.{Connection, PreparedStatement}

import io.eels.Row
import io.eels.schema.{ArrayType, DataType, Field, StructType}

trait JdbcDialect {
  def setObject(index: Int, value: Any, field: Field, stmt: PreparedStatement, conn: Connection): Unit = {
    field.dataType match {
      case ArrayType(innerType) =>

        val sqlType = toJdbcType(innerType).toLowerCase.split("\\(")(0)

        val array = conn.createArrayOf(sqlType, value.asInstanceOf[Seq[AnyRef]].toArray)
        stmt.setArray(index + 1, array)

      case _ =>
        stmt.setObject(index + 1, value)
    }
  }

  def create(schema: StructType, table: String): String

  def insert(row: Row, table: String): String

  def toJdbcType(field: Field): String

  def toJdbcType(dataType: DataType): String

  def fromJdbcType(column: Int, metadata: java.sql.ResultSetMetaData): DataType

  // accepts a raw value from the jdbc driver and returns an appropriate neutral type
  def sanitize(value: Any): Any

  /**
    * Returns a parameterized insert query
    */
  def insertQuery(schema: StructType, table: String): String
}

object JdbcDialect {
  def apply(url: String): JdbcDialect = {
    if (url.toLowerCase.startsWith("jdbc:oracle")) new OracleJdbcDialect()
    else new GenericJdbcDialect()
  }
}

