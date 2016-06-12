package io.eels

import io.eels.component.SchemaInferrer
import io.eels.component.SchemaRule
import io.eels.component.csv.CsvSource
import io.eels.component.csv.Header
import io.eels.schema.Column
import io.eels.schema.ColumnType
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import java.nio.file.Paths

class SchemaInferrerTest : WordSpec() {

  init {

    val file = javaClass.getResource("/csvtest.csv").toURI()
    val path = Paths.get(file)

    "SchemaInferrer" should {
      "use rules to infer column types" {
        val inferrer = SchemaInferrer(ColumnType.String, SchemaRule("a", ColumnType.Int, false), SchemaRule("b", ColumnType.Boolean))
        CsvSource(path).withHeader(Header.FirstRow).withSchemaInferrer(inferrer).schema() shouldBe Schema(listOf(
            Column("a", ColumnType.Int, false),
            Column("b", ColumnType.Boolean, true),
            Column("c", ColumnType.String, true)
        ))
      }
    }
  }
}