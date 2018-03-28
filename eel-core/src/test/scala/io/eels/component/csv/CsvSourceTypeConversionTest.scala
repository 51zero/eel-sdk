package io.eels.component.csv

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import io.eels.schema._
import org.scalatest.{Ignore, Matchers, WordSpec}

@Ignore
class CsvSourceTypeConversionTest extends WordSpec with Matchers {
  "CsvSource" should {
    "read schema" in {
      val exampleCsvString =
        """A,B,C,D
          |1,2.2,3,foo
          |4,5.5,6,bar
        """.stripMargin

      val stream = new ByteArrayInputStream(exampleCsvString.getBytes(StandardCharsets.UTF_8))
      val schema = new StructType(Vector(
        Field("A", IntType.Signed),
        Field("B", DoubleType),
        Field("C", IntType.Signed),
        Field("D", StringType)
      ))
      val source = new CsvSource(() => stream)
        .withSchema(schema)
      /*
              .withSchemaInferrer(SchemaInferrer(StringType, DataTypeRule("A", IntType.Signed)))
              .withSchemaInferrer(SchemaInferrer(StringType, DataTypeRule("B", DoubleType)))
              .withSchemaInferrer(SchemaInferrer(StringType, DataTypeRule("C", IntType.Signed)))
      */
      source.schema.fields.foreach(println)
      val ds = source.toDataStream()
      val firstRow = ds.iterator.toIterable.head
      val firstRowA = firstRow.get("A")
      println(firstRowA) // prints 1 as expected
      println(firstRowA.getClass.getTypeName) // prints java.lang.String
      assert(firstRowA == 1) // this assertion will fail because firstRowA is not an Int
    }
  }
}
