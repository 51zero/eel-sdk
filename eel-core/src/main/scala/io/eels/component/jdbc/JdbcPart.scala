package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import com.sksamuel.exts.io.Using
import com.sksamuel.exts.metrics.Timed
import io.eels.component.jdbc.dialect.JdbcDialect
import io.eels.datastream.Subscriber
import io.eels.{Part, Row}

class JdbcPart(connFn: () => Connection,
               query: String,
               bindFn: (PreparedStatement) => Unit = stmt => (),
               fetchSize: Int = 100,
               dialect: JdbcDialect
              ) extends Part with Timed with JdbcPrimitives with Using {

  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
    using(connFn()) { conn =>

      try {

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

        iterator.grouped(1000).foreach(subscriber.next)
        subscriber.completed()
      } catch {
        case t: Throwable => subscriber.error(t)
      }
    }
  }
}
