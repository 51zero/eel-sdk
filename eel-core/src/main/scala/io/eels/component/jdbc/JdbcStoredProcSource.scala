package io.eels.component.jdbc

import java.sql._

import com.sksamuel.scalax.metrics.Timed
import io.eels._

case class JdbcStoredProcSource(url: String,
                                storedProcedure: String,
                                createStmtFn: Connection => CallableStatement,
                                params: Seq[Any],
                                fetchSize: Int = 100,
                                providedSchema: Option[Schema] = None,
                                providedDialect: Option[JdbcDialect] = None)
  extends AbstractJdbcSource(url, providedSchema, providedDialect) with Timed {

  def withProvidedSchema(schema: Schema): JdbcStoredProcSource = copy(providedSchema = Some(schema))
  def withProvidedDialect(dialect: JdbcDialect): JdbcStoredProcSource = copy(providedDialect = Some(dialect))

  private def setup(): (Connection, CallableStatement) = {
    val conn = connect()
    val stmt = createStmtFn(conn)
    stmt.setFetchSize(fetchSize)
    for ((param, index) <- params.zipWithIndex) {
      stmt.setObject(index, param)
    }
    (conn, stmt)
  }

  override def parts: Seq[Part] = {

    val (conn, stmt) = setup()
    logger.debug(s"Executing stored procedure [$storedProcedure]...")
    val rs = timed("Stored proc") {
      stmt.execute()
      stmt.getResultSet
    }

    val schema = schemaFor(rs)
    val part = new JdbcPart(rs, stmt, conn, schema)
    Seq(part)
  }

  override protected def fetchSchema: Schema = {

    val (conn, stmt) = setup()
    logger.debug(s"Executing stored procedure to retrieve schema [$storedProcedure]...")
    val rs = timed("Stored proc") {
      stmt.execute()
      stmt.getResultSet
    }

    val schema = schemaFor(rs)
    rs.close()
    stmt.close()
    conn.close()

    logger.debug("Fetched schema: ")
    logger.debug(schema.print)
    schema
  }
}



