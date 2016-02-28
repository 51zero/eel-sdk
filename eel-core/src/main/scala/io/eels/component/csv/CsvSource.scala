package io.eels.component.csv

import java.nio.file.Path

import com.github.tototoshi.csv.{CSVFormat, CSVReader, QUOTE_MINIMAL, Quoting}
import com.sksamuel.scalax.io.Using
import io.eels._

trait CsvFormat {
  val delimiter: Char
  val quoteChar: Char
  val escapeChar: Char
  val lineTerminator: String
}

trait DefaultCsvFormat extends CsvFormat {
  override val delimiter: Char = ','
  override val quoteChar: Char = '"'
  override val escapeChar: Char = '"'
  override val lineTerminator: String = "\r\n"
}

object DefaultCsvFormat extends DefaultCsvFormat

case class CsvSource(path: Path,
                     overrideSchema: Option[Schema] = None,
                     format: CsvFormat = DefaultCsvFormat,
                     inferrer: SchemaInferrer = StringInferrer,
                     hasHeader: Boolean = true) extends Source with Using {

  def withDelimiter(c: Char): CsvSource = copy(format = new CsvFormat {
    override val delimiter: Char = c
    override val quoteChar: Char = format.quoteChar
    override val escapeChar: Char = format.escapeChar
    override val lineTerminator: String = format.lineTerminator
  })

  implicit val csvFormat: CSVFormat = new CSVFormat {
    override val delimiter: Char = format.delimiter
    override val quoteChar: Char = format.quoteChar
    override val treatEmptyLineAsNil: Boolean = false
    override val escapeChar: Char = format.escapeChar
    override val lineTerminator: String = format.lineTerminator
    override val quoting: Quoting = QUOTE_MINIMAL
  }

  def withSchemaInferrer(inferrer: SchemaInferrer): CsvSource = copy(inferrer = inferrer)
  def withHeader(header: Boolean): CsvSource = copy(hasHeader = header)
  def withSchema(schema: Schema): CsvSource = copy(overrideSchema = Some(schema))
  def withFormat(format: CsvFormat): CsvSource = copy(format = format)

  override def schema: Schema = overrideSchema.getOrElse {
    val reader = CSVReader.open(path.toFile)
    using(reader) { reader =>
      val headers = reader.readNext().get
      if (hasHeader)
        inferrer(headers)
      else
        inferrer(List.tabulate(headers.size)(_.toString))
    }
  }

  override def readers: Seq[Reader] = {

    val reader = CSVReader.open(path.toFile)
    val iter = reader.iterator

    if (hasHeader) {
      if (iter.hasNext)
        iter.next
    }

    val part = new Reader {

      override def close(): Unit = reader.close()

      override def iterator: Iterator[InternalRow] = new Iterator[InternalRow] {

        override def hasNext: Boolean = {
          val hasNext = iter.hasNext
          if (!hasNext)
            reader.close()
          hasNext
        }

        override def next: InternalRow = iter.next
      }
    }

    Seq(part)
  }
}

trait SchemaInferrer {
  def apply(headers: Seq[String]): Schema
}

object StringInferrer extends SchemaInferrer {
  def apply(headers: Seq[String]): Schema = Schema(headers.map(header => Column(header, SchemaType.String, true)).toList)
}

case class SchemaRule(pattern: String, schemaType: SchemaType, nullable: Boolean = true) {
  def apply(header: String): Option[Column] = {
    if (header.matches(pattern)) Some(Column(header, schemaType, nullable)) else None
  }
}

object SchemaInferrer {

  import com.sksamuel.scalax.OptionImplicits._

  def apply(default: SchemaType, first: SchemaRule, rest: SchemaRule*): SchemaInferrer = new SchemaInferrer {
    val rules = first +: rest
    override def apply(headers: Seq[String]): Schema = {
      val columns = headers.map { header =>
        rules.foldLeft(none[Column]) { (schemaType, rule) =>
          schemaType.orElse(rule(header))
        }.getOrElse(Column(header, default, true))
      }
      Schema(columns.toList)
    }
  }
}