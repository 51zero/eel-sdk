package io.eels.component.jdbc

import io.eels.util.Logging
import io.eels.schema.Schema
import io.eels.util.Timed
import io.eels.component.Part
import io.eels.component.Using
import io.eels.util.Option
import io.eels.util.getOrElse

class JdbcSource(url: String,
                 val query: String,
                 val fetchSize: Int = 100,
                 providedSchema: Option<Schema> = Option.None,
                 providedDialect: Option<JdbcDialect> = Option.None,
                 val bucketing: Option<Bucketing> = Option.None) : AbstractJdbcSource(url, providedSchema, providedDialect), Logging, Using, Timed {

  override fun schema(): Schema = providedSchema.getOrElse { fetchSchema() }

  fun withProvidedSchema(schema: Schema): JdbcSource = JdbcSource(url = url, query = query, fetchSize = fetchSize, providedDialect = providedDialect, bucketing = bucketing, providedSchema = Option(schema))
  fun withProvidedDialect(dialect: JdbcDialect): JdbcSource = JdbcSource(url = url, query = query, fetchSize = fetchSize, providedDialect = Option(dialect), bucketing = bucketing, providedSchema = providedSchema)

  override fun parts(): List<Part> {

    val conn = connect()
    val stmt = conn.createStatement()
    stmt.fetchSize = fetchSize

    val rs = timed("Executing query") {
      stmt.executeQuery(query)
    }

    val schema = schemaFor(rs)
    val part = ResultsetPart(rs, stmt, conn, schema)
    return listOf(part)
  }

  override fun fetchSchema(): Schema {
    return using(connect()) { conn ->
      using(conn.createStatement()) { stmt ->

        stmt.fetchSize = fetchSize

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

data class Bucketing(val columnName: String, val numberOfBuckets: Int)