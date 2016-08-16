package io.eels.component.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import com.sksamuel.exts.metrics.Timed
import io.eels.schema.Schema
import io.eels.{Part, Source}

object JdbcSource {
  def apply(url: String, query: String): JdbcSource = JdbcSource(() => DriverManager.getConnection(url), query)
}

case class JdbcSource(connFn: () => Connection,
                      query: String,
                      bind: (PreparedStatement) => Unit = stmt => (),
                      fetchSize: Int = 100,
                      providedSchema: Option[Schema] = None,
                      providedDialect: Option[JdbcDialect] = None,
                      bucketing: Option[Bucketing] = None)
  extends Source with JdbcPrimitives with Logging with Using with Timed {

  override def schema(): Schema = providedSchema.getOrElse(fetchSchema())

  def withBind(bind: (PreparedStatement) => Unit) = copy(bind = bind)
  def withFetchSize(fetchSize: Int): JdbcSource = copy(fetchSize = fetchSize)
  def withProvidedSchema(schema: Schema): JdbcSource = copy(providedSchema = Option(schema))
  def withProvidedDialect(dialect: JdbcDialect): JdbcSource = copy(providedDialect = Option(dialect))

  private def dialect(): JdbcDialect = providedDialect.getOrElse(new GenericJdbcDialect())

  override def parts(): List[Part] = {
    val conn = connFn()
    val stmt = conn.prepareStatement(query)
    stmt.setFetchSize(fetchSize)
    bind(stmt)

    val rs = timed("Executing query $query") {
      stmt.executeQuery()
    }

    val schema = schemaFor(dialect(), rs)
    val part = new ResultsetPart(rs, stmt, conn, schema)
    List(part)
  }

  def fetchSchema(): Schema = {
    using(connFn()) { conn =>
      val schemaQuery = s"SELECT * FROM ($query) tmp WHERE 1=0"
      using(conn.prepareStatement(schemaQuery)) { stmt =>

        stmt.setFetchSize(fetchSize)
        bind(stmt)

        val rs = timed("Executing query $query") {
          stmt.executeQuery()
        }

        val schema = schemaFor(dialect(), rs)
        rs.close()
        schema
      }
    }
  }
}

case class Bucketing(columnName: String, numberOfBuckets: Int)
