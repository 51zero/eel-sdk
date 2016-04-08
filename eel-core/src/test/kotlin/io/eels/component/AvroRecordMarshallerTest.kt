package io.eels.component

import io.eels.Column
import io.eels.Row
import io.eels.Schema
import io.eels.component.avro.AvroRecordMarshaller
import io.kotlintest.specs.WordSpec
import org.apache.avro.SchemaBuilder

class AvroRecordMarshallerTest : WordSpec() {

  val avroSchema = SchemaBuilder.record("row").fields().requiredString("s").requiredLong("l").requiredBoolean("b").endRecord()
  val eelSchema = Schema(Column("a"), Column("b"), Column("c"))
  val marshaller = AvroRecordMarshaller(avroSchema)

  init {
    "ConvertingAvroRecordMarshaller" should {
      "create record with all fields set in field order" with {
        val record = marshaller.toRecord(Row(eelSchema, listOf("a", 1L, false)))
        record.get("s") shouldBe "a"
        record.get("l") shouldBe 1L
        record.get("b") shouldBe false
      }
      "only accept rows with same number of values as schema fields" with {
        expecting(IllegalArgumentException::class) {
          val eelSchema = Schema(Column("a"), Column("b"))
          marshaller.toRecord(Row(eelSchema, listOf("a", 1L)))
        }
        expecting(IllegalArgumentException::class) {
          val eelSchema = Schema(Column("a"), Column("b"), Column("c"), Column("d"))
          marshaller.toRecord(Row(eelSchema, listOf("1", "2", "3", "4")))
        }
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