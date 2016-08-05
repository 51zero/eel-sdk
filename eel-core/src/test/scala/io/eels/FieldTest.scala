package io.eels

import io.eels.schema.Field

class FieldTest extends WordSpec with Matchers {

    "Field.toLowerCase" should {
      "return new field with lower case name" {
        val col = Field("MyName")
        col.toLowerCase().name shouldBe "myname"
      }
    }
  }
