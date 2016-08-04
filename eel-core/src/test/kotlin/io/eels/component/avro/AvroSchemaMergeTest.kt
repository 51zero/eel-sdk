package io.eels.component.avro

import io.kotlintest.specs.StringSpec
import org.apache.avro.SchemaBuilder

class AvroSchemaMergeTest : StringSpec() {
  init {
    "AvroSchemaMerge should merge all fields" {
      val schema1 = SchemaBuilder.record("record1").fields().nullableString("str1", "moo").requiredFloat("f").endRecord()
      val schema2 = SchemaBuilder.record("record2").fields().nullableString("str2", "foo").requiredFloat("g").endRecord()
      AvroSchemaMerge("finalname", "finalnamespace", listOf(schema1, schema2)) shouldBe
          SchemaBuilder.record("finalname").namespace("finalnamespace")
              .fields()
              .nullableString("str1", "moo")
              .requiredFloat("f")
              .nullableString("str2", "foo")
              .requiredFloat("g")
              .endRecord()
    }

    "AvroSchemaMerge should drop duplicates" {
      val schema1 = SchemaBuilder.record("record1").fields().nullableString("str1", "moo").requiredFloat("f").endRecord()
      val schema2 = SchemaBuilder.record("record2").fields().nullableString("str2", "foo").requiredFloat("f").endRecord()
      AvroSchemaMerge("finalname", "finalnamespace", listOf(schema1, schema2)) shouldBe
          SchemaBuilder.record("finalname").namespace("finalnamespace")
              .fields()
              .nullableString("str1", "moo")
              .requiredFloat("f")
              .nullableString("str2", "foo")
              .endRecord()
    }
  }
}