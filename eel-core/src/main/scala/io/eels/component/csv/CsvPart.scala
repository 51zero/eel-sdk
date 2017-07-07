package io.eels.component.csv

import java.io.Closeable

import com.sksamuel.exts.Logging
import com.univocity.parsers.csv.CsvParser
import io.eels.component.FlowableIterator
import io.eels.schema.StructType
import io.eels.{Flow, Part, Row}
import io.reactivex.Flowable
import io.reactivex.disposables.Disposable
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Try

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

  override def open2(): Flow = {

    val parser = createParser()
    val input = fs.open(path)
    parser.beginParsing(input)

    val iterator = Iterator.continually(parser.parseNext).takeWhile(_ != null).drop(rowsToSkip).map { records =>
      Row(schema, records.toVector)
    }

    val closeable = new Closeable {
      override def close(): Unit = Try {
        parser.stopParsing()
        input.close()
      }
    }

    Flow(closeable, iterator.grouped(100))
  }

  override def open(): Flowable[Row] = {

    val parser = createParser()
    val input = fs.open(path)
    parser.beginParsing(input)

    val iterator = Iterator.continually(parser.parseNext).takeWhile(_ != null).drop(rowsToSkip).map { records =>
      Row(schema, records.toVector)
    }

    val disposable = new Disposable {
      override def dispose(): Unit = Try {
        parser.stopParsing()
        input.close()
      }
      override def isDisposed: Boolean = false
    }

    FlowableIterator(iterator, disposable)
  }
}