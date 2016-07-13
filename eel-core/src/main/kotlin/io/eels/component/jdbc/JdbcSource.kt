package io.eels.component.jdbc

import io.eels.Source
import io.eels.util.Logging
import io.eels.schema.Schema
import io.eels.util.Timed
import io.eels.component.Part
import io.eels.component.Using
import io.eels.util.Option
import io.eels.util.getOrElse

data class JdbcSource(val url: String,
                      val query: String,
                      val fetchSize: Int = 100,
                      val providedSchema: Option<Schema> = Option.None,
                      val providedDialect: Option<JdbcDialect> = Option.None,
                      val bucketing: Option<Bucketing> = Option.None) : Source, JdbcPrimitives, Logging, Using, Timed {

  override fun schema(): Schema = providedSchema.getOrElse { fetchSchema() }

  fun withProvidedSchema(schema: Schema): JdbcSource = copy(providedSchema = Option(schema))
  fun withProvidedDialect(dialect: JdbcDialect): JdbcSource = copy(providedDialect = Option(dialect))
  fun withFetchSize(fetchSize: Int): JdbcSource = copy(fetchSize = fetchSize)

  private fun dialect(): JdbcDialect = providedDialect.getOrElse(GenericJdbcDialect())

  override fun parts(): List<Part> {

    val conn = connect(url)
    val stmt = conn.createStatement()
    stmt.fetchSize = fetchSize

    val rs = timed("Executing query") {
      stmt.executeQuery(query)
    }

    val schema = schemaFor(url, dialect(), rs)
    val part = ResultsetPart(rs, stmt, conn, schema)
    return listOf(part)
  }

  fun fetchSchema(): Schema {
    return using(connect(url)) { conn ->
      using(conn.createStatement()) { stmt ->

        stmt.fetchSize = fetchSize

        val schemaQuery = "SELECT * FROM ($query) tmp WHERE 1=0"
        val rs = timed("Query for schema [$schemaQuery]...") {
          stmt.executeQuery(schemaQuery)
        }

        val schema = schemaFor(url, dialect(), rs)
        rs.close()
        schema
      }
    }
  }
}

data class Bucketing(val columnName: String, val numberOfBuckets: Int)