package io.eels.component.jdbc

import java.io.Closeable
import java.sql.{Connection, PreparedStatement}

import com.sksamuel.exts.metrics.Timed
import io.eels.{Flow, Part, Row}

import scala.util.Try

class JdbcPart(connFn: () => Connection,
               query: String,
               bindFn: (PreparedStatement) => Unit = stmt => (),
               fetchSize: Int = 100,
               dialect: JdbcDialect
              ) extends Part with Timed with JdbcPrimitives {

  override def open(): Flow = {

    val conn = connFn()
    val stmt = conn.prepareStatement(query)
    stmt.setFetchSize(fetchSize)
    bindFn(stmt)

    val rs = timed(s"Executing query $query") {
      stmt.executeQuery()
    }

    val schema = schemaFor(dialect, rs)

    val iterator = new Iterator[Row] {
      override def hasNext: Boolean = rs.next()
      override def next(): Row = {
        val values = schema.fieldNames().map(name => rs.getObject(name))
        Row(schema, values)
      }
    }

    val closeable = new Closeable {
      override def close(): Unit = {
        logger.debug(s"Closing result set on jdbc part $query")
        Try {
          rs.close()
        }
        Try {
          conn.close()
        }
      }
    }

    Flow(closeable, iterator)
  }
}
