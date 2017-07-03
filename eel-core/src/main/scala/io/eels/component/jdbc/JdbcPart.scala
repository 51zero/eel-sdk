package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import com.sksamuel.exts.TryOrLog
import com.sksamuel.exts.metrics.Timed
import io.eels.component.FlowableIterator
import io.eels.component.jdbc.dialect.JdbcDialect
import io.eels.{Part, Row}
import io.reactivex.Flowable
import io.reactivex.disposables.Disposable

class JdbcPart(connFn: () => Connection,
               query: String,
               bindFn: (PreparedStatement) => Unit = stmt => (),
               fetchSize: Int = 100,
               dialect: JdbcDialect
              ) extends Part with Timed with JdbcPrimitives {

  override def open(): Flowable[Row] = {

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
        val values = schema.fieldNames().map { name =>
          val raw = rs.getObject(name)
          dialect.sanitize(raw)
        }
        Row(schema, values)
      }
    }

    val disposable = new Disposable {
      override def isDisposed: Boolean = conn.isClosed
      override def dispose(): Unit = {
        logger.debug(s"Closing result set on jdbc part $query")
        TryOrLog {
          rs.close()
        }
        TryOrLog {
          conn.close()
        }
      }
    }

    FlowableIterator(iterator, disposable)
  }
}
