package io.eels.component.avro

import org.apache.avro.SchemaBuilder
import org.scalatest.{Matchers, WordSpec}

class ConvertingAvroRecordMarshallerTest extends WordSpec with Matchers {

  val schema = SchemaBuilder.record("row").fields().requiredString("s").requiredLong("l").requiredBoolean("b").endRecord()
  val marshaller = new ConvertingAvroRecordMarshaller(schema)

  "ConvertingAvroRecordMarshaller" should {
    "create record with all fields set in field order" in {
      val record = marshaller.toRecord(Seq("a", 1l, false))
      record.get("s") shouldBe "a"
      record.get("l") shouldBe 1l
      record.get("b") shouldBe false
    }
    "only accept rows with same number of values as schema fields" in {
      assertThrows[RuntimeException] {
        marshaller.toRecord(Seq("1", "2"))
      }
      assertThrows[RuntimeException] {
        marshaller.toRecord(Seq("1", "2", "3", "4"))
      }
    }
    "convert strings to longs" in {
      val record = marshaller.toRecord(Seq("1", "2", "true"))
      record.get("l") shouldBe 2l
    }
    "convert strings to booleans" in {
      val record = marshaller.toRecord(Seq("1", "2", "true"))
      record.get("b") shouldBe true
    }
  }
}
