package io.eels.component.avro

import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.{Matchers, WordSpec}

class ConverterTest extends WordSpec with Matchers {

  "Converter" should {
    "convert to long" in {
      AvroConverter(SchemaBuilder.builder().longType()).convert("123") shouldBe 123l
      AvroConverter(SchemaBuilder.builder().longType()).convert(14555) shouldBe 14555l
    }
    "convert to String" in {
      AvroConverter(SchemaBuilder.builder().stringType()).convert(123l) shouldBe "123"
      AvroConverter(SchemaBuilder.builder().stringType).convert(124) shouldBe "124"
      AvroConverter(SchemaBuilder.builder().stringType).convert("Qweqwe") shouldBe "Qweqwe"
    }
    "convert to boolean" in {
      AvroConverter(SchemaBuilder.builder().booleanType).convert(true) shouldBe true
      AvroConverter(SchemaBuilder.builder().booleanType).convert(false) shouldBe false
      AvroConverter(SchemaBuilder.builder().booleanType).convert("true") shouldBe true
      AvroConverter(SchemaBuilder.builder().booleanType()).convert("false") shouldBe false
    }
    "convert to Double" in {
      AvroConverter(SchemaBuilder.builder().doubleType).convert("213.4") shouldBe 213.4d
      AvroConverter(SchemaBuilder.builder().doubleType).convert("345.11") shouldBe 345.11d
      AvroConverter(SchemaBuilder.builder().doubleType()).convert(345) shouldBe 345.0
    }
  }
}
