package io.eels

import java.nio.file.Paths

import io.eels.component.csv.{CsvSource, Header}
import io.eels.schema.{Field, FieldType, Schema}
import org.scalatest.{Matchers, WordSpec}

class SchemaInferrerTest extends WordSpec with Matchers {

  val file = getClass.getResource("/io/eels/component/csv/csvtest.csv").toURI()
  val path = Paths.get(file)

  "SchemaInferrer" should {
    "use rules to infer column types" in {
      val inferrer = SchemaInferrer(FieldType.String, SchemaRule("a", FieldType.Int, false), SchemaRule("b", FieldType.Boolean))
      CsvSource(path).withHeader(Header.FirstRow).withSchemaInferrer(inferrer).schema() shouldBe Schema(
        Field("a", FieldType.Int, false),
        Field("b", FieldType.Boolean, true),
        Field("c", FieldType.String, true)
      )
    }
  }
}