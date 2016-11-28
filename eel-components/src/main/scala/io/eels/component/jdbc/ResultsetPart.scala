package io.eels.component.jdbc

import java.sql.{Connection, ResultSet, Statement}

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import io.eels.{Part, Row}
import io.reactivex.functions.Consumer
import io.reactivex.{Emitter, Flowable}

import scala.util.control.NonFatal

/**
 * A Part for a Resultset. Will publish all rows from the resultset and then close the resultset.
 */
class ResultsetPart(val rs: ResultSet,
                    val stmt: Statement,
                    val conn: Connection,
                    val schema: StructType) extends Part with Logging {

  override def data(): Flowable[Row] = Flowable.generate(new Consumer[Emitter[Row]] {

    override def accept(e: Emitter[Row]): Unit = {
      try {
        if (rs.next()) {
          val values = schema.fieldNames().map(name => rs.getObject(name))
          val row = Row(schema, values)
          e.onNext(row)
        } else {
          e.onComplete()
        }
      } catch {
        case NonFatal(t) => e.onError(t)
      } finally {
        rs.close()
      }
    }
  })
}
