package io.eels.component.csv

import com.univocity.parsers.csv.CsvParser
import io.eels.Row
import io.eels.schema.Schema
import io.eels.component.Part
import rx.Observable
import java.nio.file.Path

class CsvPart(val createParser: () -> CsvParser,
              val path: Path,
              val header: Header,
              val verifyRows: Boolean,
              val schema: Schema) : Part {

  override fun data(): Observable<Row> {

    val parser = createParser()
    parser.beginParsing(path.toFile())

    val rowsToSkip: Int = when (header) {
      Header.FirstRow -> 1
      else -> 0
    }

//    val iterator = object : AbstractIterator<Array<String>>() {
//      override fun computeNext() {
//        val values = parser.parseNext()
//        if (values == null) done()
//        else setNext(values)
//      }
//    }

    return Observable.create { sub ->
      sub.onStart()
      var next: Array<String>? = parser.parseNext()
      var toDrop: Int = rowsToSkip
      while (next != null) {
        val row = Row(schema, next.asList())
        if (toDrop == 0)
          sub.onNext(row)
        else
          toDrop -= 1
        next = parser.parseNext()
      }
      if (!sub.isUnsubscribed)
        sub.onCompleted()
    }
  }
}