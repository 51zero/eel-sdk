package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import com.sksamuel.exts.io.Using
import com.sksamuel.exts.metrics.Timed
import io.eels.component.jdbc.dialect.JdbcDialect
import io.eels.datastream.Subscriber
import io.eels.{Part, Row}

import scala.collection.mutable.ArrayBuffer

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

        val buffer = new ArrayBuffer[Row](100)
        while (rs.next) {
          val values = schema.fieldNames().map { name =>
            val raw = rs.getObject(name)
            dialect.sanitize(raw)
          }
          buffer append Row(schema, values)
          if (buffer.size == 100) {
            subscriber.next(buffer.toVector)
            buffer.clear()
          }
        }

        if (buffer.nonEmpty)
          subscriber.next(buffer.toVector)

        subscriber.completed()

      } catch {
        case t: Throwable => subscriber.error(t)
      }
    }
  }
}
