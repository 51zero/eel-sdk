package io.eels.component.jdbc

import io.eels.util.Logging
import io.eels.Row
import io.eels.schema.Schema
import io.eels.component.Part
import rx.Observable
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

/**
 * A Part for a Resultset. Will publish all rows from the resultset and then close the resultset.
 */
class ResultsetPart(val rs: ResultSet,
                    val stmt: Statement,
                    val conn: Connection,
                    val schema: Schema) : Part, Logging {

  override fun data(): Observable<Row> {
    return Observable.create<Row> {
      try {
        it.onStart()
        while (rs.next()) {
          val values = schema.columnNames().map { rs.getObject(it) }
          val row = Row(schema, values)
          it.onNext(row)
        }
        it.onCompleted()
      } catch(t: Throwable) {
        it.onError(t)
      } finally {
        conn.close()
      }
    }
  }
}
