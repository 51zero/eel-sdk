package io.eels.component.csv

import com.sksamuel.exts.io.Using
import com.typesafe.config.{Config, ConfigFactory}
import io.eels._
import io.eels.component.csv.CsvPredef._
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

case class CsvSource(path: Path,
                     overrideSchema: Option[StructType] = None,
                     format: CsvFormat = CsvFormat(),
                     inferrer: SchemaInferrer = StringInferrer,
                     ignoreLeadingWhitespaces: Boolean = true,
                     ignoreTrailingWhitespaces: Boolean = true,
                     skipEmptyLines: Boolean = true,
                     emptyCellValue: String = null,
                     nullValue: String = null,
                     skipBadRows: Option[Boolean] = None,
                     header: Header = Header.FirstRow,
                     skipRows: Option[Long] = None,
                     selectedColumns: Seq[String] = Seq.empty,
                     skipRowCallback: Option[SkipRowCallback] = None)
                    (implicit fs: FileSystem) extends Source with Using {

  val config: Config = ConfigFactory.load()
  val defaultSkipBadRows = config.getBoolean("eel.csv.skipBadRows")

  def withSchemaInferrer(inferrer: SchemaInferrer): CsvSource = copy(inferrer = inferrer)

  // sets whether this source has a header and if so where to read from
  def withHeader(header: Header): CsvSource = copy(header = header)

  def withSchema(schema: StructType): CsvSource = copy(overrideSchema = Some(schema))

  def withDelimiter(c: Char): CsvSource = copy(format = format.copy(delimiter = c))

  def withQuoteChar(c: Char): CsvSource = copy(format = format.copy(quoteChar = c))

  def withQuoteEscape(c: Char): CsvSource = copy(format = format.copy(quoteEscape = c))

  def withFormat(format: CsvFormat): CsvSource = copy(format = format)

  // use this value when the cell/record is empty quotes in the source data
  def withEmptyCellValue(emptyCellValue: String): CsvSource = copy(emptyCellValue = emptyCellValue)

  // use this value when the cell/record is empty in the source data
  def withNullValue(nullValue: String): CsvSource = copy(nullValue = nullValue)

  def withSkipEmptyLines(skipEmptyLines: Boolean): CsvSource = copy(skipEmptyLines = skipEmptyLines)

  def withIgnoreLeadingWhitespaces(ignore: Boolean): CsvSource = copy(ignoreLeadingWhitespaces = ignore)

  def withIgnoreTrailingWhitespaces(ignore: Boolean): CsvSource = copy(ignoreTrailingWhitespaces = ignore)

  def withSkipRows(count: Long): CsvSource = copy(skipRows = Some(count))

  def withSkipBadRows(skipBadRows: Option[Boolean]): CsvSource = copy(skipBadRows = skipBadRows)

  def withSkipRowCallback(callback: Option[SkipRowCallback]):CsvSource = copy( skipRowCallback = callback)

  private def createParser() = {
    CsvSupport.createParser(format,
      ignoreLeadingWhitespaces,
      ignoreTrailingWhitespaces,
      skipEmptyLines,
      emptyCellValue,
      nullValue,
      skipRows,
      selectedColumns)
  }

  override def schema(): StructType = overrideSchema.getOrElse {
    val parser = createParser()
    val input = fs.open(path)
    parser.beginParsing(input)
    val headers = header match {
      case Header.None =>
        // read the first row just to get the count of columns, then we'll call them column 1,2,3,4 etc
        // todo change the column labels to a,b,c,d
        val records = parser.parseNext()
        (0 until records.size).map(_.toString).toList
      case Header.FirstComment =>
        while (parser.getContext.lastComment() == null && parser.parseNext() != null) {
        }
        val str = Option(parser.getContext.lastComment).getOrElse("")
        str.split(format.delimiter).toList
      case Header.FirstRow => parser.parseNext().toList
    }
    parser.stopParsing()
    inferrer.schemaOf(headers)
  }

  override def parts(): List[Part] = {
    val part = new CsvPart(() => createParser, path, header, skipBadRows.getOrElse(defaultSkipBadRows), schema(), skipRowCallback)
    List(part)
  }
}

object CsvSource {
  def apply(path: java.nio.file.Path): CsvSource = CsvSource(new Path(path.toString))(FileSystem.getLocal(new Configuration))
}