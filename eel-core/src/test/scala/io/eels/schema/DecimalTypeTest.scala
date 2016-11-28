package io.eels.schema

import org.scalatest.{Matchers, WordSpec}

class DecimalTypeTest extends WordSpec with Matchers {

  "decimal type" should {
    "match on wildcard" in {
      DecimalType(Scale(1), Precision(2)) matches DecimalType(Scale(-1), Precision(2)) shouldBe true
      DecimalType(Scale(1), Precision(2)) matches DecimalType(Scale(1), Precision(-1)) shouldBe true
      DecimalType(Scale(1), Precision(2)) matches DecimalType(Scale(-1), Precision(-1)) shouldBe true
      DecimalType(Scale(1), Precision(2)) matches DecimalType.Wildcard shouldBe true
    }
    "match on values" in {
      DecimalType(Scale(1), Precision(2)) matches DecimalType(Scale(1), Precision(2)) shouldBe true
      DecimalType(Scale(1), Precision(2)) matches DecimalType(Scale(1), Precision(3)) shouldBe false
      DecimalType(Scale(1), Precision(2)) matches DecimalType(Scale(2), Precision(2)) shouldBe false
    }
  }
}
