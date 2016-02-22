package io.eels.component.jdbc

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Row, Column, FrameSchema, SchemaType}

trait JdbcDialect {
  def create(schema: FrameSchema, table: String): String
  def insert(row: Row, schema: FrameSchema, table: String): String
  def toJdbcType(column: Column): String
  def fromJdbcType(i: Int): SchemaType

  /**
    * Returns a parameterized insert query
    */
  def insertQuery(schema: FrameSchema, table: String): String

}

object JdbcDialect {
  /**
    * Detect dialect from the connection string
    */
  def apply(url: String): JdbcDialect = GenericJdbcDialect
}

object GenericJdbcDialect extends GenericJdbcDialect

trait GenericJdbcDialect extends JdbcDialect with StrictLogging {

  override def toJdbcType(column: Column): String = column.`type` match {
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

  override def fromJdbcType(i: Int): SchemaType = i match {
    case java.sql.Types.BIGINT        => SchemaType.Long
    case java.sql.Types.BINARY        => SchemaType.Binary
    case java.sql.Types.BIT           => SchemaType.Boolean
    case java.sql.Types.BLOB          => SchemaType.Binary
    case java.sql.Types.BOOLEAN       => SchemaType.Boolean
    case java.sql.Types.CHAR          => SchemaType.String
    case java.sql.Types.CLOB          => SchemaType.String
    case java.sql.Types.DATALINK      => SchemaType.Unsupported
    case java.sql.Types.DATE          => SchemaType.Date
    case java.sql.Types.DECIMAL       => SchemaType.Decimal
    case java.sql.Types.DISTINCT      => SchemaType.Unsupported
    case java.sql.Types.DOUBLE        => SchemaType.Double
    case java.sql.Types.FLOAT         => SchemaType.Float
    case java.sql.Types.INTEGER       => SchemaType.Int
    case java.sql.Types.JAVA_OBJECT   => SchemaType.Unsupported
    case java.sql.Types.LONGNVARCHAR  => SchemaType.String
    case java.sql.Types.LONGVARBINARY => SchemaType.Binary
    case java.sql.Types.LONGVARCHAR   => SchemaType.String
    case java.sql.Types.NCHAR         => SchemaType.String
    case java.sql.Types.NCLOB         => SchemaType.String
    case java.sql.Types.NULL          => SchemaType.Unsupported
    case java.sql.Types.NUMERIC       => SchemaType.Decimal
    case java.sql.Types.NVARCHAR      => SchemaType.String
    case java.sql.Types.OTHER         => SchemaType.Unsupported
    case java.sql.Types.REAL          => SchemaType.Double
    case java.sql.Types.REF           => SchemaType.String
    case java.sql.Types.ROWID         => SchemaType.Long
    case java.sql.Types.SMALLINT      => SchemaType.Int
    case java.sql.Types.SQLXML        => SchemaType.String
    case java.sql.Types.STRUCT        => SchemaType.String
    case java.sql.Types.TIME          => SchemaType.Timestamp
    case java.sql.Types.TIMESTAMP     => SchemaType.Timestamp
    case java.sql.Types.TINYINT       => SchemaType.Int
    case java.sql.Types.VARBINARY     => SchemaType.Binary
    case java.sql.Types.VARCHAR       => SchemaType.String
    case _                            => SchemaType.Unsupported
  }

  override def create(schema: FrameSchema, table: String): String = {
    val columns = schema.columns.map(c => s"${c.name} ${toJdbcType(c)}").mkString("(", ",", ")")
    s"CREATE TABLE $table $columns"
  }

  override def insertQuery(schema: FrameSchema, table: String): String = {
    val columns = schema.columnNames.mkString(",")
    val parameters = List.fill(schema.columns.size)("?").mkString(",")
    s"INSERT INTO $table ($columns) VALUES ($parameters)"
  }

  override def insert(row: Row, schema: FrameSchema, table: String): String = {
    val columns = schema.columnNames.mkString(",")
    val values = row.mkString("'", "','", "'")
    s"INSERT INTO $table ($columns) VALUES ($values)"
  }
}