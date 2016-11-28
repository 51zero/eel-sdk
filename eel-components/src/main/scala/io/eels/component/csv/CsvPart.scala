package io.eels.component.csv

import com.sksamuel.exts.Logging
import com.univocity.parsers.csv.CsvParser
import io.eels.schema.StructType
import io.eels.{Part, Row}
import io.reactivex.functions.Consumer
import io.reactivex.{Emitter, Flowable}
import org.apache.hadoop.fs.{FileSystem, Path}

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

  override def data(): Flowable[Row] = Flowable.generate(new Consumer[Emitter[Row]] {

    val parser = createParser()
    val input = fs.open(path)
    parser.beginParsing(input)

    val iterator = Iterator.continually(parser.parseNext).takeWhile(_ != null).drop(rowsToSkip)

    override def accept(e: Emitter[Row]): Unit = {
      try {
        if (iterator.hasNext) {
          val record = iterator.next()
          val row = Row(schema, record.toVector)
          e.onNext(row)
        } else {
          e.onComplete()
        }
      } catch {
        case NonFatal(t) => e.onError(t)
      } finally {
        parser.stopParsing()
        input.close()
      }
    }
  })
}