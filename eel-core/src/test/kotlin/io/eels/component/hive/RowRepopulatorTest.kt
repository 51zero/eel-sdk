package io.eels.component.hive

import io.eels.Row
import io.eels.schema.Field
import io.eels.schema.Schema
import io.kotlintest.specs.StringSpec

class RowRepopulatorTest : StringSpec() {
  init {
    "RowRepopulatorFn should reorder in line with target schema" {
      val row = Row(Schema(Field("a"), Field("b"), Field("c")), listOf("aaaa", "bbb", "ccc"))
      val targetSchema = Schema(Field("c"), Field("b"))
      rowRepopulator(row, targetSchema) shouldBe Row(Schema(Field("c"), Field("b")), listOf("ccc", "bbb"))
    }
    "RowRepopulatorFn should lookup missing data" {
      val row = Row(Schema(Field("a"), Field("b"), Field("c")), listOf("aaaa", "bbb", "ccc"))
      val targetSchema = Schema(Field("c"), Field("d"))
      rowRepopulator(row, targetSchema, mapOf("d" to "ddd")) shouldBe Row(Schema(Field("c"), Field("d")), listOf("ccc", "ddd"))
    }
  }
}