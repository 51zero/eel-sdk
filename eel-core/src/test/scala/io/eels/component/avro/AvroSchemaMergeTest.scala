package io.eels.component.avro

import org.apache.avro.SchemaBuilder
import org.scalatest.{Matchers, WordSpec}

class AvroSchemaMergeTest extends WordSpec with Matchers {
  "AvroSchemaMerge" should {
    "merge all fields" in {
      val schema1 = SchemaBuilder.record("record1").fields().nullableString("str1", "moo").requiredFloat("f").endRecord()
      val schema2 = SchemaBuilder.record("record2").fields().nullableString("str2", "foo").requiredFloat("g").endRecord()
      AvroSchemaMerge("finalname", "finalnamespace", List(schema1, schema2)) shouldBe
          SchemaBuilder.record("finalname").namespace("finalnamespace")
              .fields()
              .nullableString("str1", "moo")
              .requiredFloat("g")
              .nullableString("str2", "foo")
              .requiredFloat("f")
              .endRecord()
    }

    "drop duplicates" in {
      val schema1 = SchemaBuilder.record("record1").fields().nullableString("str1", "moo").requiredFloat("f").endRecord()
      val schema2 = SchemaBuilder.record("record2").fields().nullableString("str2", "foo").requiredFloat("f").endRecord()
      AvroSchemaMerge("finalname", "finalnamespace", List(schema1, schema2)) shouldBe
          SchemaBuilder.record("finalname").namespace("finalnamespace")
              .fields()
              .nullableString("str1", "moo")
              .nullableString("str2", "foo")
              .requiredFloat("f")
              .endRecord()
    }
  }
}