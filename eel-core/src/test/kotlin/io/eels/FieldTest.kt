package io.eels

import io.eels.schema.Field
import io.kotlintest.specs.WordSpec

class FieldTest : WordSpec() {

  init {
    "Column.toLowerCase" should {
      "return new column with lower case name" {
        val col = Field("MyName")
        col.toLowerCase().name shouldBe "myname"
      }
    }
  }
}