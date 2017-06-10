package io.eels.component.avro

import org.apache.avro.SchemaBuilder
import org.scalatest.{Matchers, WordSpec}

class ConverterTest extends WordSpec with Matchers {

  "Converter" should {
    "convert to long" in {
      AvroSerializer(SchemaBuilder.builder().longType()).serialize("123") shouldBe 123l
      AvroSerializer(SchemaBuilder.builder().longType()).serialize(14555) shouldBe 14555l
    }
    "convert to String" in {
      AvroSerializer(SchemaBuilder.builder().stringType()).serialize(123l) shouldBe "123"
      AvroSerializer(SchemaBuilder.builder().stringType).serialize(124) shouldBe "124"
      AvroSerializer(SchemaBuilder.builder().stringType).serialize("Qweqwe") shouldBe "Qweqwe"
    }
    "convert to boolean" in {
      AvroSerializer(SchemaBuilder.builder().booleanType).serialize(true) shouldBe true
      AvroSerializer(SchemaBuilder.builder().booleanType).serialize(false) shouldBe false
      AvroSerializer(SchemaBuilder.builder().booleanType).serialize("true") shouldBe true
      AvroSerializer(SchemaBuilder.builder().booleanType()).serialize("false") shouldBe false
    }
    "convert to Double" in {
      AvroSerializer(SchemaBuilder.builder().doubleType).serialize("213.4") shouldBe 213.4d
      AvroSerializer(SchemaBuilder.builder().doubleType).serialize("345.11") shouldBe 345.11d
      AvroSerializer(SchemaBuilder.builder().doubleType()).serialize(345) shouldBe 345.0
    }
  }
}
