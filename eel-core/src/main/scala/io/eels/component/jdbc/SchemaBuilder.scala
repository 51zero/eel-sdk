package io.eels.component.jdbc

import java.sql.{ResultSetMetaData, ResultSet}

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{FrameSchema, Column}

object SchemaBuilder extends StrictLogging {

  def apply(rs: ResultSet, dialect: JdbcDialect): FrameSchema = {
    logger.debug("Building frame schema from resultset")

    val md: ResultSetMetaData = rs.getMetaData
    val columnCount = md.getColumnCount
    logger.debug(s"Resultset column count is $columnCount")

    val cols = for ( k <- 1 to columnCount ) yield {
      Column(
        name = md.getColumnLabel(k),
        `type` = dialect.fromJdbcType(md.getColumnType(k)),
        nullable = md.isNullable(k) == 1,
        precision = md.getPrecision(k),
        scale = md.getScale(k),
        signed = md.isSigned(k),
        None
      )
    }

    FrameSchema(cols.toList)
  }
}
