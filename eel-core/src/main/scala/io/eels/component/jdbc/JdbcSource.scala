package io.eels.component.jdbc

import com.sksamuel.scalax.Logging
import com.sksamuel.scalax.io.Using
import com.sksamuel.scalax.metrics.Timed
import io.eels._

case class JdbcSource(url: String,
                      query: String,
                      fetchSize: Int = 100,
                      providedSchema: Option[Schema] = None,
                      providedDialect: Option[JdbcDialect] = None)
  extends AbstractJdbcSource(url, providedSchema, providedDialect) with Logging with Using with Timed {

  def withProvidedSchema(schema: Schema): JdbcSource = copy(providedSchema = Some(schema))
  def withProvidedDialect(dialect: JdbcDialect): JdbcSource = copy(providedDialect = Some(dialect))

  override def parts: Seq[Part] = {

    val conn = connect()
    val stmt = conn.createStatement()
    stmt.setFetchSize(fetchSize)

    val rs = timed("Executing query") {
      stmt.executeQuery(query)
    }

    val schema = schemaFor(rs)
    val part = new JdbcPart(rs, stmt, conn, schema)
    Seq(part)
  }

  override protected def fetchSchema: Schema = {

    val conn = connect()
    val stmt = conn.createStatement()
    stmt.setFetchSize(fetchSize)

    val schemaQuery = s"SELECT * FROM ($query) tmp WHERE 1=0"
    val rs = timed(s"Query for schema [$schemaQuery]...") {
      stmt.executeQuery(schemaQuery)
    }

    val schema = schemaFor(rs)
    rs.close()
    stmt.close()
    conn.close()

    schema
  }
}