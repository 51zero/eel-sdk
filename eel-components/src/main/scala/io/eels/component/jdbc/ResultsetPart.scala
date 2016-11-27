package io.eels.component.jdbc

import java.sql.{Connection, ResultSet, Statement}

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import io.eels.{Part, Row}
import rx.lang.scala.Observable

/**
 * A Part for a Resultset. Will publish all rows from the resultset and then close the resultset.
 */
class ResultsetPart(val rs: ResultSet,
                    val stmt: Statement,
                    val conn: Connection,
                    val schema: StructType) extends Part with Logging {

  override def data(): Observable[Row] = {
    Observable.apply { subscriber =>
      try {
        subscriber.onStart()
        while (rs.next()) {
          val values = schema.fieldNames().map(name => rs.getObject(name))
          val row = Row(schema, values)
          subscriber.onNext(row)
        }
        subscriber.onCompleted()
      } catch {
        case t: Throwable =>
          subscriber.onError(t)
      } finally {
        conn.close()
      }
    }
  }
}
