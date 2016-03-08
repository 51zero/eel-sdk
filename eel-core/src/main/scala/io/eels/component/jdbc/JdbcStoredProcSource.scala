package io.eels.component.jdbc

import java.sql._

import com.sksamuel.scalax.metrics.Timed
import io.eels._

case class JdbcStoredProcSource(url: String,
                                storedProcedure: String,
                                params: Seq[Any],
                                fetchSize: Int = 100,
                                providedSchema: Option[Schema] = None,
                                providedDialect: Option[JdbcDialect] = None)
  extends AbstractJdbcSource(url, providedSchema, providedDialect) with Timed {

  def withProvidedSchema(schema: Schema): JdbcStoredProcSource = copy(providedSchema = Some(schema))
  def withProvidedDialect(dialect: JdbcDialect): JdbcStoredProcSource = copy(providedDialect = Some(dialect))

  private def setup(): (Connection, CallableStatement) = {
    val conn = connect()
    val stmt = conn.prepareCall(storedProcedure)
    stmt.setFetchSize(fetchSize)
    for ((param, index) <- params.zipWithIndex) {
      stmt.setObject(index, param)
    }
    (conn, stmt)
  }

  override def parts: Seq[Part] = {

    val (conn, stmt) = setup()
    logger.debug(s"Executing stored procedure [proc=$storedProcedure, params=${params.mkString(",")}]")
    val rs = timed("Stored proc") {
      val result = stmt.execute()
      logger.debug(s"Stored proc result=$result")
      stmt.getResultSet
    }

    val schema = schemaFor(rs)
    val part = new JdbcPart(rs, stmt, conn, schema)
    Seq(part)
  }

  override protected def fetchSchema: Schema = {

    val (conn, stmt) = setup()

    try {

      logger.debug(s"Executing stored procedure for schema [proc=$storedProcedure, params=${params.mkString(",")}]")
      val rs = timed("Stored proc") {
        val result = stmt.execute()
        logger.debug(s"Stored proc result=$result")
        stmt.getResultSet
      }
      val schema = schemaFor(rs)
      rs.close()
      schema

    } finally {
      stmt.close()
      conn.close()
    }
  }
}



