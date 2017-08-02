package io.eels

import java.util.Date

import io.eels.schema.{BooleanType, DateType, DoubleType, Field, StringType, StructType}
import org.scalatest.{Matchers, WordSpec}

class RowUtilsTest extends WordSpec with Matchers {
  "RowUtils.rowAlign" should {
    "rowAlign should reorder in line with target schema" in {
      val row = Row(StructType(Field("a"), Field("b"), Field("c")), "aaaa", "bbb", "ccc")
      val targetSchema = StructType(Field("c"), Field("b"))
      RowUtils.rowAlign(row, targetSchema) shouldBe Row(StructType(Field("c"), Field("b")), "ccc", "bbb")
    }
    "rowAlign should lookup missing data" in {
      val row = Row(StructType(Field("a"), Field("b"), Field("c")), "aaaa", "bbb", "ccc")
      val targetSchema = StructType(Field("c"), Field("d"))
      RowUtils.rowAlign(row, targetSchema, Map("d" -> "ddd")) shouldBe Row(StructType(Field("c"), Field("d")), "ccc", "ddd")
    }
    "rowAlign should throw an error if a field is missing" in {
      val row = Row(StructType(Field("a"), Field("b"), Field("c")), "aaaa", "bbb", "ccc")
      val targetSchema = StructType(Field("c"), Field("d"))
      intercept[RuntimeException] {
        RowUtils.rowAlign(row, targetSchema)
      }
    }
  }

  "RowUtils.coerce" should {
    "coerce values to match types" in {
      val schema = StructType(Field("a", StringType), Field("b", DoubleType), Field("c", BooleanType), Field("d", DateType))
      val row = Row(schema, Vector(1, "1.4", "true", "1123123244493"))
      RowUtils.coerce(row) shouldBe Row(schema, "1", 1.4D, true, new Date(1123123244493L))
    }
  }
}