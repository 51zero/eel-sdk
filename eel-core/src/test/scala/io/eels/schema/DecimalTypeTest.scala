package io.eels.schema

import org.scalatest.{Matchers, WordSpec}

class DecimalTypeTest extends WordSpec with Matchers {

  "decimal type" should {
    "match on wildcard" in {
      DecimalType(Precision(2), Scale(1)) matches DecimalType(Precision(-1), Scale(1)) shouldBe true
      DecimalType(Precision(2), Scale(1)) matches DecimalType(Precision(2), Scale(-1)) shouldBe true
      DecimalType(Precision(2), Scale(1)) matches DecimalType(Precision(-1), Scale(-1)) shouldBe true
      DecimalType(Precision(2), Scale(1)) matches DecimalType.Wildcard shouldBe true
    }
    "match on values" in {
      DecimalType(Precision(2), Scale(1)) matches DecimalType(Precision(2), Scale(1)) shouldBe true
      DecimalType(Precision(2), Scale(1)) matches DecimalType(Precision(3), Scale(3)) shouldBe false
      DecimalType(Precision(2), Scale(1)) matches DecimalType(Precision(2), Scale(2)) shouldBe false
    }
  }
}
