package io.eels.component.jdbc

import java.sql.{Connection, ResultSet, Statement}
import java.util.function.Consumer

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import io.eels.{Part, Row}
import reactor.core.publisher.{Flux, FluxSink}

import scala.util.control.NonFatal

/**
 * A Part for a Resultset. Will publish all rows from the resultset and then close the resultset.
 */
class ResultsetPart(val rs: ResultSet,
                    val stmt: Statement,
                    val conn: Connection,
                    val schema: StructType) extends Part with Logging {

  override def data(): Flux[Row] = Flux.create(new Consumer[FluxSink[Row]] {
    override def accept(sink: FluxSink[Row]): Unit = {
      try {
        while (rs.next && !sink.isCancelled) {
          val values = schema.fieldNames().map(name => rs.getObject(name))
          val row = Row(schema, values)
          sink.next(row)
        }
        sink.complete()
      } catch {
        case NonFatal(error) =>
          logger.warn("Could not read file", error)
          sink.error(error)
      } finally {
        rs.close()
      }
    }
  }, FluxSink.OverflowStrategy.BUFFER)
}
