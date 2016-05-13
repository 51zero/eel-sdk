package io.eels.component.jdbc

import io.eels.Schema
import io.eels.Timed
import io.eels.component.Part
import io.eels.zipWithIndex
import java.sql.CallableStatement
import java.sql.Connection

class JdbcStoredProcSource(url: String,
                           val storedProcedure: String,
                           val params: List<Any>,
                           val fetchSize: Int = 100,
                           providedSchema: Schema?,
                           providedDialect: JdbcDialect?) : AbstractJdbcSource(url, providedSchema, providedDialect), Timed {

  override fun schema(): Schema = providedSchema ?: fetchSchema()

  fun withProvidedSchema(schema: Schema): JdbcStoredProcSource = JdbcStoredProcSource(url = url, storedProcedure = storedProcedure, params = params, providedSchema = schema, providedDialect = providedDialect)
  fun withProvidedDialect(dialect: JdbcDialect): JdbcStoredProcSource = JdbcStoredProcSource(url = url, storedProcedure = storedProcedure, params = params, providedSchema = providedSchema, providedDialect = dialect)

  private fun setup(): Pair<Connection, CallableStatement> {
    val conn = connect()
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

    val schema = schemaFor(rs)
    val part = ResultsetPart(rs, stmt, conn, schema)
    return listOf(part)
  }

  override fun fetchSchema(): Schema {

    val (conn, stmt) = setup()

    try {

      logger.debug("Executing stored procedure for schema [proc=$storedProcedure, params=${params.joinToString(",")}]")
      val rs = timed("Stored proc") {
        val result = stmt.execute()
        logger.debug("Stored proc result=$result")
        stmt.resultSet
      }
      val schema = schemaFor(rs)
      rs.close()
      return schema

    } finally {
      stmt.close()
      conn.close()
    }
  }
}



