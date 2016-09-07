package io.eels.component.avro

import io.eels.schema.Field
import io.eels.Row
import io.eels.schema.Schema
import org.apache.avro.SchemaBuilder
import org.scalatest.{Matchers, WordSpec}

class AvroRecordSerializerTest extends WordSpec with Matchers {

  val avroSchema = SchemaBuilder.record("row").fields().requiredString("s").requiredLong("l").requiredBoolean("b").endRecord()
  val serializer = new AvroRecordSerializer(avroSchema)

  "AvroRecordMarshaller" should {
    "createReader field from values in row" in {
      val eelSchema = Schema(Field("s"), Field("l"), Field("b"))
      val record = serializer.toRecord(Row(eelSchema, "a", 1L, false))
      record.get("s") shouldBe "a"
      record.get("l") shouldBe 1L
      record.get("b") shouldBe false
    }
    "only accept rows with same number of values as schema fields" in {
      intercept[IllegalArgumentException] {
        val eelSchema = Schema(Field("a"), Field("b"))
        serializer.toRecord(Row(eelSchema, "a", 1L))
      }
      intercept[IllegalArgumentException] {
        val eelSchema = Schema(Field("a"), Field("b"), Field("c"), Field("d"))
        serializer.toRecord(Row(eelSchema, "1", "2", "3", "4"))
      }
    }
    "support out of order rows" in {
      val eelSchema = Schema(Field("l"), Field("b"), Field("s"))
      val record = serializer.toRecord(Row(eelSchema, 1L, false, "a"))
      record.get("s") shouldBe "a"
      record.get("l") shouldBe 1L
      record.get("b") shouldBe false
    }
    "convert strings to longs" in {
      val record = serializer.toRecord(Row(AvroSchemaFns.fromAvroSchema(avroSchema), "1", "2", "true"))
      record.get("l") shouldBe 2L
    }
    "convert strings to booleans" in {
      val record = serializer.toRecord(Row(AvroSchemaFns.fromAvroSchema(avroSchema), "1", "2", "true"))
      record.get("b") shouldBe true
    }
    "convert longs to strings" in {
      val record = serializer.toRecord(Row(AvroSchemaFns.fromAvroSchema(avroSchema), 1L, "2", "true"))
      record.get("s") shouldBe "1"
    }
    "convert booleans to strings" in {
      val record = serializer.toRecord(Row(AvroSchemaFns.fromAvroSchema(avroSchema), true, "2", "true"))
      record.get("s") shouldBe "true"
    }
  }
}

//    "AvroRecordFn" should
//        in {
//          "replace missing values if flag set" in in {
//            val schema = Schema(Column("a"), Column("b"), Column("c"))
//            toRecord(listOf("1", "3"), schema, Schema(Column("a"), Column("c")), config).toString shouldBe
//                """{"a": "1", "b": null, "c": "3"}"""
//          }
//          }
//