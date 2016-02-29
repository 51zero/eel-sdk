package io.eels

import org.scalatest.{Matchers, WordSpec}

class ConverterTest extends WordSpec with Matchers {

  "Converter" should {
    "convert to long" in {
      Converter(SchemaType.Long)("123") shouldBe 123l
      Converter(SchemaType.Long)(14555) shouldBe 14555l
    }
    "convert to String" in {
      Converter(SchemaType.String)(123l) shouldBe "123"
      Converter(SchemaType.String)(124) shouldBe "124"
      Converter(SchemaType.String)("Qweqwe") shouldBe "Qweqwe"
    }
    "convert to boolean" in {
      Converter(SchemaType.Boolean)(true) shouldBe true
      Converter(SchemaType.Boolean)(false) shouldBe false
      Converter(SchemaType.Boolean)("true") shouldBe true
      Converter(SchemaType.Boolean)("false") shouldBe false
    }
    "convert to Double" in {
      Converter(SchemaType.Double)("213.4") shouldBe 213.4d
      Converter(SchemaType.Double)("345.11") shouldBe 345.11d
      Converter(SchemaType.Double)(345) shouldBe 345.0
    }
  }
}
