package io.eels.component.jdbc

import io.eels.Source
import io.eels.schema.Schema
import io.eels.util.Timed
import io.eels.component.Part
import io.eels.util.Option
import io.eels.util.getOrElse
import io.eels.util.zipWithIndex
import java.sql.CallableStatement
import java.sql.Connection

data class JdbcStoredProcSource(val url: String,
                                val storedProcedure: String,
                                val params: List<Any>,
                                val fetchSize: Int = 100,
                                val providedSchema: Option<Schema> = Option.None,
                                val providedDialect: Option<JdbcDialect> = Option.None) : Source, JdbcPrimitives, Timed {

  override fun schema(): Schema = providedSchema.getOrElse { fetchSchema() }

  fun withProvidedSchema(schema: Schema): JdbcStoredProcSource = copy(providedSchema = Option(schema), providedDialect = providedDialect)
  fun withDialect(dialect: JdbcDialect): JdbcStoredProcSource = copy(providedDialect = Option(dialect))

  private fun dialect(): JdbcDialect = providedDialect.getOrElse(GenericJdbcDialect())

  private fun setup(): Pair<Connection, CallableStatement> {
    val conn = connect(url)
    val stmt = conn.prepareCall(storedProcedure)
    stmt.fetchSize = fetchSize
    for ((param, index) in params.zipWithIndex()) {
      stmt.setObject(index + 1, param)
    }
    return Pair(conn, stmt)
  }

  override fun parts(): List<Part> {

    val (conn, stmt) = setup()
    logger.debug("Executing stored procedure [proc=$storedProcedure, params=${params.joinToString(",")}]")
    val rs = timed("Stored proc") {
      val result = stmt.execute()
      logger.debug("Stored proc result=$result")
      stmt.resultSet
    }

    val schema = schemaFor(url, dialect(), rs)
    val part = ResultsetPart(rs, stmt, conn, schema)
    return listOf(part)
  }

  fun fetchSchema(): Schema {

    val (conn, stmt) = setup()

    try {

      logger.debug("Executing stored procedure for schema [proc=$storedProcedure, params=${params.joinToString(",")}]")
      val rs = timed("Stored proc") {
        val result = stmt.execute()
        logger.debug("Stored proc result=$result")
        stmt.resultSet
      }
      val schema = schemaFor(url, dialect(), rs)
      rs.close()
      return schema

    } finally {
      stmt.close()
      conn.close()
    }
  }
}



