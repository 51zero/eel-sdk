package io.eels.component.avro

import io.eels.schema.Field
import io.eels.Row
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import org.apache.avro.SchemaBuilder

class AvroRecordSerializerTest : WordSpec() {

  val avroSchema = SchemaBuilder.record("row").fields().requiredString("s").requiredLong("l").requiredBoolean("b").endRecord()
  val marshaller = AvroRecordSerializer(avroSchema)

  init {

    "AvroRecordMarshaller" should {
      "createReader field from values in row" {
        val eelSchema = Schema(Field("s"), Field("l"), Field("b"))
        val record = marshaller.toRecord(Row(eelSchema, listOf("a", 1L, false)))
        record.get("s") shouldBe "a"
        record.get("l") shouldBe 1L
        record.get("b") shouldBe false
      }
      "only accept rows with same number of values as schema fields" {
        shouldThrow<IllegalArgumentException> {
          val eelSchema = Schema(Field("a"), Field("b"))
          marshaller.toRecord(Row(eelSchema, listOf("a", 1L)))
        }
        shouldThrow<IllegalArgumentException> {
          val eelSchema = Schema(Field("a"), Field("b"), Field("c"), Field("d"))
          marshaller.toRecord(Row(eelSchema, listOf("1", "2", "3", "4")))
        }
      }
      "support out of order rows" {
        val eelSchema = Schema(Field("l"), Field("b"), Field("s"))
        val record = marshaller.toRecord(Row(eelSchema, listOf(1L, false, "a")))
        record.get("s") shouldBe "a"
        record.get("l") shouldBe 1L
        record.get("b") shouldBe false
      }
      "convert strings to longs"  {
        val record = marshaller.toRecord(Row(AvroSchemaFns.fromAvroSchema(avroSchema), listOf("1", "2", "true")))
        record.get("l") shouldBe 2L
      }
      "convert strings to booleans"  {
        val record = marshaller.toRecord(Row(AvroSchemaFns.fromAvroSchema(avroSchema), listOf("1", "2", "true")))
        record.get("b") shouldBe true
      }
      "convert longs to strings"  {
        val record = marshaller.toRecord(Row(AvroSchemaFns.fromAvroSchema(avroSchema), listOf(1L, "2", "true")))
        record.get("s") shouldBe "1"
      }
      "convert booleans to strings"  {
        val record = marshaller.toRecord(Row(AvroSchemaFns.fromAvroSchema(avroSchema), listOf(true, "2", "true")))
        record.get("s") shouldBe "true"
      }
    }
  }
}

//    "AvroRecordFn" should
//        {
//          "replace missing values if flag set" in {
//            val schema = Schema(Column("a"), Column("b"), Column("c"))
//            toRecord(listOf("1", "3"), schema, Schema(Column("a"), Column("c")), config).toString shouldBe
//                """{"a": "1", "b": null, "c": "3"}"""
//          }
//          }
//