package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}
import java.util.function.Consumer

import com.sksamuel.exts.metrics.Timed
import io.eels.{CloseableIterator, Part, Row}
import reactor.core.Disposable
import reactor.core.publisher.{Flux, FluxSink}

import scala.util.Try
import scala.util.control.NonFatal

class JdbcPart(connFn: () => Connection,
               query: String,
               bindFn: (PreparedStatement) => Unit = stmt => (),
               fetchSize: Int = 100,
               dialect: JdbcDialect
              ) extends Part with Timed with JdbcPrimitives {

  override def flux(): Flux[Row] = {

    val conn = connFn()
    val stmt = conn.prepareStatement(query)
    stmt.setFetchSize(fetchSize)
    bindFn(stmt)

    val rs = timed(s"Executing query $query") {
      stmt.executeQuery()
    }

    val schema = schemaFor(dialect, rs)

    Flux.create(new Consumer[FluxSink[Row]] {
      override def accept(t: FluxSink[Row]): Unit = {

        t.onDispose(new Disposable {
          override def dispose(): Unit = {
            Try { rs.close() }
            Try { conn.close() }
          }
        })

        try {
          while (rs.next) {
            val values = schema.fieldNames().map(name => rs.getObject(name))
            val row = Row(schema, values)
            t.next(row)
          }
          t.complete()
        } catch {
          case NonFatal(e) => t.error(e)
        } finally {
          Try { rs.close() }
          Try { conn.close() }
        }
      }
    })
  }

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
