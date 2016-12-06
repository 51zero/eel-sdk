package io.eels.component.csv

import com.sksamuel.exts.Logging
import com.univocity.parsers.csv.CsvParser
import io.eels.schema.StructType
import io.eels.{Part2, PartStream, Row}
import org.apache.hadoop.fs.{FileSystem, Path}

class CsvPart(val createParser: () => CsvParser,
              val path: Path,
              val header: Header,
              val skipBadRows: Boolean,
              val schema: StructType)
             (implicit fs: FileSystem) extends Part2 with Logging {

  val rowsToSkip: Int = header match {
    case Header.FirstRow => 1
    case _ => 0
  }

  override def stream(): PartStream = new PartStream {

    private val parser = createParser()
    private val input = fs.open(path)
    private var closed = false

    parser.beginParsing(input)

    private val iterator = Iterator.continually(parser.parseNext).takeWhile(_ != null).drop(rowsToSkip).map { records =>
      Row(schema, records.toVector)
    }.grouped(1000).withPartial(true)

    override def next(): List[Row] = iterator.next()
    override def hasNext(): Boolean = !closed && iterator.hasNext

    override def close(): Unit = {
      parser.stopParsing()
      input.close()
      closed = true
    }
  }
}