package io.eels.component.csv

import java.io.Closeable

import com.sksamuel.exts.Logging
import com.univocity.parsers.csv.CsvParser
import io.eels.schema.StructType
import io.eels.{CloseIterator, CloseableIterator, Part, Row}
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

  override def iterator2(): CloseIterator[Row] = {

    val parser = createParser()
    val input = fs.open(path)
    parser.beginParsing(input)

    val closeable = new Closeable {
      override def close(): Unit = Try {
        parser.stopParsing()
        input.close()
      }
    }

    val iterator = Iterator.continually(parser.parseNext).takeWhile(_ != null).drop(rowsToSkip).map { records =>
      Row(schema, records.toVector)
    }

    CloseIterator(closeable, iterator)
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

    override val iterator: Iterator[Seq[Row]] = Iterator.continually(parser.parseNext).takeWhile(_ != null).drop(rowsToSkip).map {
      records =>
        Row(schema, records.toVector)
    }.grouped(1000).withPartial(true)
  }
}