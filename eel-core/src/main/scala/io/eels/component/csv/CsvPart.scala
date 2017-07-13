package io.eels.component.csv

import java.io.InputStream

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import com.univocity.parsers.csv.CsvParser
import io.eels.datastream.Subscriber
import io.eels.schema.StructType
import io.eels.{Part, Row}
import org.apache.hadoop.fs.FileSystem

class CsvPart(createParser: () => CsvParser,
              inputFn: () => InputStream,
              header: Header,
              skipBadRows: Boolean,
              schema: StructType)
             (implicit fs: FileSystem) extends Part with Logging with Using {

  val rowsToSkip: Int = header match {
    case Header.FirstRow => 1
    case _ => 0
  }

  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
    using(inputFn()) { input =>

      val parser = createParser()

      try {

        parser.beginParsing(input)

        val iterator = Iterator.continually(parser.parseNext).takeWhile(_ != null).drop(rowsToSkip).map { records =>
          Row(schema, records.toVector)
        }

        iterator.grouped(1000).foreach(subscriber.next)
        subscriber.completed()

      } catch {
        case t: Throwable => subscriber.error(t)
      } finally {
        parser.stopParsing()
        input.close()
      }
    }
  }
}