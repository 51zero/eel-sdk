package io.eels.component.csv

import java.util.function.Consumer

import com.sksamuel.exts.Logging
import com.univocity.parsers.csv.CsvParser
import io.eels.schema.StructType
import io.eels.{CloseableIterator, Part, Row}
import org.apache.hadoop.fs.{FileSystem, Path}
import reactor.core.Disposable
import reactor.core.publisher.{Flux, FluxSink}

import scala.util.Try
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

  override def flux(): Flux[Row] = {
    Flux.create(new Consumer[FluxSink[Row]] {
      override def accept(t: FluxSink[Row]): Unit = {

        val parser = createParser()
        val input = fs.open(path)
        parser.beginParsing(input)

        def close(): Unit = {
          Try {
            parser.stopParsing()
            input.close()
          }
        }

        t.onDispose(new Disposable {
          override def dispose(): Unit = close()
        })

        try {
          Iterator.continually(parser.parseNext).takeWhile(_ != null).drop(rowsToSkip).foreach { records =>
            val row = Row(schema, records.toVector)
            t.next(row)
          }
          t.complete()
        } catch {
          case NonFatal(e) => t.error(e)
        } finally {
          close()
        }
      }
    })
  }

  override def iterator(): CloseableIterator[Seq[Row]] = new CloseableIterator[Seq[Row]] {

    private val parser = createParser()
    private val input = fs.open(path)
    private var closed = false

    parser.beginParsing(input)

    override def close(): Unit = {
      parser.stopParsing()
      input.close()
      super.close()
    }

    override val iterator: Iterator[Seq[Row]] = Iterator.continually(parser.parseNext).takeWhile(_ != null).drop(rowsToSkip).map { records =>
      Row(schema, records.toVector)
    }.grouped(1000).withPartial(true)
  }
}