package io.eels.component.jdbc

import io.eels.RowListener
import io.eels.Source
import io.eels.component.Part
import io.eels.component.Using
import io.eels.schema.Schema
import io.eels.util.Logging
import io.eels.util.Option
import io.eels.util.Timed
import io.eels.util.getOrElse
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

data class JdbcSource @JvmOverloads constructor(val connFn: () -> Connection,
                                                val query: String,
                                                val bind: (PreparedStatement) -> Any = {},
                                                val fetchSize: Int = 100,
                                                val providedSchema: Option<Schema> = Option.None,
                                                val providedDialect: Option<JdbcDialect> = Option.None,
                                                val bucketing: Option<Bucketing> = Option.None,
                                                val listener: RowListener = RowListener.Noop) : Source, JdbcPrimitives, Logging, Using, Timed {

  constructor(url: String, query: String) : this({ DriverManager.getConnection(url) }, query)

  override fun schema(): Schema = providedSchema.getOrElse { fetchSchema() }

  fun withBind(bind: (PreparedStatement) -> Any) = copy(bind = bind)
  fun withFetchSize(fetchSize: Int): JdbcSource = copy(fetchSize = fetchSize)
  fun withProvidedSchema(schema: Schema): JdbcSource = copy(providedSchema = Option(schema))
  fun withProvidedDialect(dialect: JdbcDialect): JdbcSource = copy(providedDialect = Option(dialect))
  fun withListener(listener: RowListener) = copy(listener = listener)

  private fun dialect(): JdbcDialect = providedDialect.getOrElse(GenericJdbcDialect())

  override fun parts(): List<Part> {

    val conn = connFn()
    val stmt = conn.prepareStatement(query)
    stmt.fetchSize = fetchSize
    bind(stmt)

    val rs = timed("Executing query $query") {
      stmt.executeQuery()
    }

    val schema = schemaFor(dialect(), rs)
    val part = ResultsetPart(rs, stmt, conn, schema, listener)
    return listOf(part)
  }

  fun fetchSchema(): Schema {
    return using(connFn()) { conn ->
      using(conn.createStatement()) { stmt ->

        stmt.fetchSize = fetchSize

        val schemaQuery = "SELECT * FROM ($query) tmp WHERE 1=0"
        val rs = timed("Query for schema [$schemaQuery]...") {
          stmt.executeQuery(schemaQuery)
        }

        val schema = schemaFor(dialect(), rs)
        rs.close()
        schema
      }
    }
  }

  data class Bucketing(val columnName: String, val numberOfBuckets: Int)
}