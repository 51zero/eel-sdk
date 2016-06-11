package io.eels.component.csv

import com.univocity.parsers.csv.CsvParser
import io.eels.Row
import io.eels.schema.Schema
import io.eels.component.Part
import one.util.streamex.StreamEx
import rx.Observable
import java.nio.file.Path

class CsvPart(val createParsernFn: () -> CsvParser,
              val path: Path,
              val header: Header,
              val verifyRows: Boolean,
              val schema: Schema) : Part {

  override fun data(): Observable<Row> {

    val parser = createParsernFn()
    parser.beginParsing(path.toFile())

    val rowsToSkip: Long = when (header) {
      Header.FirstRow -> 1L
      else -> 0L
    }

    val iterator = object : AbstractIterator<Array<String>>() {
      override fun computeNext() {
        val values = parser.parseNext()
        if (values == null) done()
        else setNext(values)
      }
    }

    return Observable.create { sub ->
      val stream = StreamEx.generate { parser.parseNext() }.takeWhile { it != null }.takeWhile { !sub.isUnsubscribed }.skip(rowsToSkip)
      sub.onStart()
      stream.forEach {
        val values = iterator.next()
        val row = Row(schema, values.asList())
        sub.onNext(row)
      }
      if (!sub.isUnsubscribed)
        sub.onCompleted()
    }
  }
}