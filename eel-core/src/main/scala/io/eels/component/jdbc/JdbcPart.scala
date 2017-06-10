package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import com.sksamuel.exts.metrics.Timed
import io.eels.{CloseableIterator, Part, Row}

import scala.util.Try

class JdbcPart(connFn: () => Connection,
               query: String,
               bindFn: (PreparedStatement) => Unit = stmt => (),
               fetchSize: Int = 100,
               dialect: JdbcDialect
              ) extends Part with Timed with JdbcPrimitives {

  override def iterator(): CloseableIterator[Seq[Row]] = new CloseableIterator[Seq[Row]] {

    private val conn = connFn()
    private val stmt = conn.prepareStatement(query)
    stmt.setFetchSize(fetchSize)
    bindFn(stmt)

    private val rs = timed(s"Executing query $query") {
      stmt.executeQuery()
    }

    private val schema = schemaFor(dialect, rs)

    override def close(): Unit = {
      Try { super.close() }
      Try { rs.close() }
      Try { conn.close() }
    }

    override val iterator: Iterator[Seq[Row]] = new Iterator[Row] {

      var _hasnext = false

      override def hasNext(): Boolean = _hasnext || {
        _hasnext = rs.next()
        _hasnext
      }

      override def next(): Row = {
        _hasnext = false
        val values = schema.fieldNames().map(name => rs.getObject(name))
        Row(schema, values)
      }

    }.grouped(fetchSize).withPartial(true)
  }
}
