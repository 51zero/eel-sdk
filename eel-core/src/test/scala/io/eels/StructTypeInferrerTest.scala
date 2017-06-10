package io.eels

import java.nio.file.Paths

import io.eels.component.csv.{CsvSource, Header}
import io.eels.schema._
import org.scalatest.{Matchers, WordSpec}

class StructTypeInferrerTest extends WordSpec with Matchers {

  val file = getClass.getResource("/io/eels/component/csv/csvtest.csv").toURI()
  val path = Paths.get(file)

  "SchemaInferrer" should {
    "use rules to infer column types" in {
      val inferrer = SchemaInferrer(StringType, DataTypeRule("a", IntType(true), false), DataTypeRule("b", BooleanType))
      CsvSource(path).withHeader(Header.FirstRow).withSchemaInferrer(inferrer).schema shouldBe StructType(
        Field("a", IntType(true), false),
        Field("b", BooleanType, true),
        Field("c", StringType, true)
      )
    }
  }
}