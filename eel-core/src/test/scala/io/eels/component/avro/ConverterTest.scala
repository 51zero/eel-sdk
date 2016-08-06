package io.eels.component.avro

import org.apache.avro.Schema
import org.scalatest.{Matchers, WordSpec}

class ConverterTest extends WordSpec with Matchers {

  "Converter" should {
    "convert to long" in {
      AvroConverter(Schema.Type.LONG).convert("123") shouldBe 123l
      AvroConverter(Schema.Type.LONG).convert(14555) shouldBe 14555l
    }
    "convert to String" in {
      AvroConverter(Schema.Type.STRING).convert(123l) shouldBe "123"
      AvroConverter(Schema.Type.STRING).convert(124) shouldBe "124"
      AvroConverter(Schema.Type.STRING).convert("Qweqwe") shouldBe "Qweqwe"
    }
    "convert to boolean" in {
      AvroConverter(Schema.Type.BOOLEAN).convert(true) shouldBe true
      AvroConverter(Schema.Type.BOOLEAN).convert(false) shouldBe false
      AvroConverter(Schema.Type.BOOLEAN).convert("true") shouldBe true
      AvroConverter(Schema.Type.BOOLEAN).convert("false") shouldBe false
    }
    "convert to Double" in {
      AvroConverter(Schema.Type.DOUBLE).convert("213.4") shouldBe 213.4d
      AvroConverter(Schema.Type.DOUBLE).convert("345.11") shouldBe 345.11d
      AvroConverter(Schema.Type.DOUBLE).convert(345) shouldBe 345.0
    }
  }
}
