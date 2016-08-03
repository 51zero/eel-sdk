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

  val rowsToSkip: Int = when (header) {
    Header.FirstRow -> 1
    else -> 0
  }

  override fun data(): Observable<Row> {

    val parser = createParser()
    parser.beginParsing(path.toFile())

    val iterator = generateSequence { parser.parseNext() }.drop(rowsToSkip)

    return Observable.create { sub ->
      sub.onStart()
      iterator.forEach {
        val row = Row(schema, it.asList())
        sub.onNext(row)
      }
      if (!sub.isUnsubscribed)
        sub.onCompleted()
    }
  }
}