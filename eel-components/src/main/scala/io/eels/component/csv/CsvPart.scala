package io.eels.component.csv

import com.sksamuel.exts.Logging
import com.univocity.parsers.csv.CsvParser
import io.eels.component.csv.CsvPredef._
import io.eels.schema.StructType
import io.eels.{CloseableIterator, Part, Row}
import org.apache.hadoop.fs.{FileSystem, Path}

class CsvPart(val createParser: () => CsvParser,
              val path: Path,
              val header: Header,
              val skipBadRows: Boolean,
              val schema: StructType,
              val skipRowCallback: Option[SkipRowCallback] = None)
             (implicit fs: FileSystem) extends Part with Logging {

  val rowsToSkip: Int = header match {
    case Header.FirstRow => 1
    case _ => 0
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

    override val iterator: Iterator[Seq[Row]] = Iterator.continually(parser.parseNext).takeWhile(_ != null)
      .drop(rowsToSkip)
      .zipWithIndex
      .flatMap { case (records, i) =>
        skipRowCallback match {
          case Some(callback) =>
            if (callback(i, schema, records)) None
            else Some(new Row(schema, records))
          case None => Some(new Row(schema, records))
        }
      }
      .grouped(1000)
      .withPartial(true)
  }
}