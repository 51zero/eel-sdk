package io.eels.component.parquet

import io.eels.Row
import io.eels.schema.{DecimalType, Field, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import scala.math.BigDecimal.RoundingMode

class DecimalWriterTest extends FunSuite {

  test("negativeDecimalTest") {
    implicit val configuration = new Configuration
    val expectedBigDecimals = Seq(BigDecimal(-5025176.39), BigDecimal(-5), BigDecimal(-999.56434), BigDecimal(-10000.9890))
    assertBigDecimals("bigd_negative.parquet", expectedBigDecimals)
  }

  test("positiveDecimalTest") {
    implicit val configuration = new Configuration
    val expectedBigDecimals = Seq(BigDecimal(5025176.39), BigDecimal(5), BigDecimal(999.56434), BigDecimal(-10000.9890))
    assertBigDecimals("bigd_positive.parquet", expectedBigDecimals)
  }

  private def assertBigDecimals(filename: String, expectedBigDecimals: Seq[BigDecimal])(implicit configuration: Configuration): Unit = {
    val schema = StructType(Field(name = "bd", dataType = DecimalType(38, 10)))
    val path = new Path(filename)
    val fileSystem = path.getFileSystem(configuration)
    if (fileSystem.exists(path)) fileSystem.delete(path, false)

    // Write out the decimal values
    val parquetWriter = RowParquetWriterFn(path = path, schema = schema, metadata = Map.empty, dictionary = false, roundingMode = RoundingMode.UP)
    expectedBigDecimals.foreach { expectedBigDecimal =>
      println(s"Writing row with value $expectedBigDecimal")
      parquetWriter.write(Row.fromMap(schema, Map("bd" -> expectedBigDecimal)))
    }
    parquetWriter.close()

    // Read back all the writes and assert their values
    val parquetProjectionSchema = ParquetSchemaFns.toParquetMessageType(schema)
    val parquetReader = RowParquetReaderFn(path, None, Option(parquetProjectionSchema), dictionaryFiltering = true)
    for (i <- 0 until expectedBigDecimals.length) {
      val readRow = parquetReader.read
      println(s"read row: $readRow")
      assert(readRow.values.head == expectedBigDecimals(i))
    }
    parquetReader.close()
  }

}
