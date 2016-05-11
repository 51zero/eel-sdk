package io.eels.component.jdbc

import io.eels.Logging
import io.eels.Schema
import io.eels.component.Part
import io.eels.component.Using

class JdbcSource(url: String,
                 val query: String,
                 val fetchSize: Int = 100,
                 providedSchema: Schema?,
                 providedDialect: JdbcDialect?,
                 val bucketing: Bucketing?) : AbstractJdbcSource(url, providedSchema, providedDialect), Logging, Using, Timed {

  override fun schema(): Schema {
    throw UnsupportedOperationException()
  }

  fun withProvidedSchema(schema: Schema): JdbcSource = copy(providedSchema = schema)
  fun withProvidedDialect(dialect: JdbcDialect): JdbcSource = copy(providedDialect = dialect)

  override fun parts(): List<Part> {

    val conn = connect()
    val stmt = conn.createStatement()
    stmt.setFetchSize(fetchSize)

    val rs = timed("Executing query") {
      stmt.executeQuery(query)
    }

    val schema = schemaFor(rs)
    val part = JdbcPart(rs, stmt, conn, schema)
    return listOf(part)
  }

  override fun fetchSchema(): Schema {
    using(connect()) { conn ->
      using(conn.createStatement) { stmt ->
        stmt.setFetchSize(fetchSize)

        val schemaQuery = "SELECT * FROM ($query) tmp WHERE 1=0"
        val rs = timed("Query for schema [$schemaQuery]...") {
          stmt.executeQuery(schemaQuery)
        }

        val schema = schemaFor(rs)
        rs.close()
        schema
      }
    }
  }
}

data class Bucketing(columnName: String, numberOfBuckets: Int)