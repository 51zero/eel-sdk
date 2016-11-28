package io.eels.component.csv

import java.util.function.Consumer

import com.sksamuel.exts.Logging
import com.univocity.parsers.csv.CsvParser
import io.eels.schema.StructType
import io.eels.{Part, Row}
import org.apache.hadoop.fs.{FileSystem, Path}
import reactor.core.publisher.{Flux, FluxSink}

import scala.util.control.NonFatal

class CsvPart(val createParser: () => CsvParser,
              val path: Path,
              val header: Header,
              val skipBadRows: Boolean,
              val schema: StructType)
             (implicit fs: FileSystem) extends Part with Logging {

  val rowsToSkip: Int = header match {
    case Header.FirstRow => 1
    case _ => 0
  }

  override def data(): Flux[Row] = Flux.create(new Consumer[FluxSink[Row]] {
    override def accept(sink: FluxSink[Row]): Unit = {

      val parser = createParser()
      val input = fs.open(path)

      try {
        parser.beginParsing(input)
        val iterator = Iterator.continually(parser.parseNext).takeWhile(_ != null).drop(rowsToSkip)
        while (iterator.hasNext && !sink.isCancelled) {
          val record = iterator.next()
          val row = Row(schema, record.toVector)
          sink.next(row)
        }
        sink.complete()
      } catch {
        case NonFatal(error) =>
          logger.warn("Could not read file", error)
          sink.error(error)
      } finally {
        parser.stopParsing()
        input.close()
      }
    }
  }, FluxSink.OverflowStrategy.BUFFER)
}