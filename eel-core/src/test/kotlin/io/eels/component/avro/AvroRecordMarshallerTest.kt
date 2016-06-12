package io.eels.component.avro

import io.eels.schema.Field
import io.eels.Row
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import org.apache.avro.SchemaBuilder

class AvroRecordMarshallerTest : WordSpec() {

  val avroSchema = SchemaBuilder.record("row").fields().requiredString("s").requiredLong("l").requiredBoolean("b").endRecord()
  val marshaller = AvroRecordMarshaller(avroSchema)

  init {
    "ConvertingAvroRecordMarshaller" should {
      "create field from values in row" {
        val eelSchema = Schema(Field("s"), Field("l"), Field("b"))
        val record = marshaller.toRecord(Row(eelSchema, listOf("a", 1L, false)))
        record.get("s") shouldBe "a"
        record.get("l") shouldBe 1L
        record.get("b") shouldBe false
      }
      "only accept rows with same number of values as schema fields" {
        expecting(IllegalArgumentException::class) {
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
      //      "convert strings to longs" with {
      //        val record = marshaller.toRecord(Seq("1", "2", "true"))
      //        record.get("l") shouldBe 2l
      //      }
      //      "convert strings to booleans" with {
      //        val record = marshaller.toRecord(Seq("1", "2", "true"))
      //        record.get("b") shouldBe true
      //      }
    }
  }
}