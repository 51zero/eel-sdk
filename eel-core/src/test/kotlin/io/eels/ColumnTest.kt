package io.eels

import io.kotlintest.specs.WordSpec

class FieldTest : WordSpec() {

  init {
    "Column.toLowerCase" should {
      "return new column with lower case name" with {
        val col = Column("MyName")
        col.toLowerCase().name shouldBe "myname"
      }
    }
  }
}