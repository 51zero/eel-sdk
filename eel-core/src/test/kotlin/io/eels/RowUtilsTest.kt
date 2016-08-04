package io.eels

import io.eels.schema.Field
import io.eels.schema.Schema
import io.kotlintest.specs.StringSpec

class RowUtilsTest : StringSpec() {
  init {
    "rowAlign should reorder in line with target schema" {
      val row = Row(Schema(Field("a"), Field("b"), Field("c")), listOf("aaaa", "bbb", "ccc"))
      val targetSchema = Schema(Field("c"), Field("b"))
      RowUtils.rowAlign(row, targetSchema) shouldBe Row(Schema(Field("c"), Field("b")), listOf("ccc", "bbb"))
    }
    "rowAlign should lookup missing data" {
      val row = Row(Schema(Field("a"), Field("b"), Field("c")), listOf("aaaa", "bbb", "ccc"))
      val targetSchema = Schema(Field("c"), Field("d"))
      RowUtils.rowAlign(row, targetSchema, mapOf("d" to "ddd")) shouldBe Row(Schema(Field("c"), Field("d")), listOf("ccc", "ddd"))
    }
    "rowAlign should throw an error if a field is missing" {
      val row = Row(Schema(Field("a"), Field("b"), Field("c")), listOf("aaaa", "bbb", "ccc"))
      val targetSchema = Schema(Field("c"), Field("d"))
      shouldThrow<IllegalStateException> {
        RowUtils.rowAlign(row, targetSchema)
      }
    }
  }
}