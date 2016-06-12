package io.eels

import io.eels.component.SchemaInferrer
import io.eels.component.SchemaRule
import io.eels.component.csv.CsvSource
import io.eels.component.csv.Header
import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import java.nio.file.Paths

class SchemaInferrerTest : WordSpec() {

  init {

    val file = javaClass.getResource("/csvtest.csv").toURI()
    val path = Paths.get(file)

    "SchemaInferrer" should {
      "use rules to infer column types" {
        val inferrer = SchemaInferrer(FieldType.String, SchemaRule("a", FieldType.Int, false), SchemaRule("b", FieldType.Boolean))
        CsvSource(path).withHeader(Header.FirstRow).withSchemaInferrer(inferrer).schema() shouldBe Schema(listOf(
            Field("a", FieldType.Int, false),
            Field("b", FieldType.Boolean, true),
            Field("c", FieldType.String, true)
        ))
      }
    }
  }
}