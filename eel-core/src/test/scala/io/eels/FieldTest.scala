package io.eels

import io.eels.schema.{Field, StringType}
import org.scalatest.{Matchers, WordSpec}

class FieldTest extends WordSpec with Matchers {
  "Field.toLowerCase" should {
    "return new field with lower case name" in {
      val col = Field("MyName", StringType)
      col.toLowerCase().name shouldBe "myname"
    }
  }
}
