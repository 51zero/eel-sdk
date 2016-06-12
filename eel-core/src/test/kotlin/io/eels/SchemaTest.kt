package io.eels

import io.eels.schema.Column
import io.eels.schema.ColumnType
import io.eels.schema.Precision
import io.eels.schema.Scale
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec

class SchemaTest : WordSpec() {

  val schema = Schema(listOf(
      Column("a", ColumnType.Boolean, signed = true, scale = Scale(22), nullable = true),
      Column("b", ColumnType.String, precision = Precision(14), signed = false, nullable = false)
  ))

  init {

    "Schema.toLowerCase" should {
      "lower case all column names" {
        Schema(listOf(Column("a"), Column("B"))).toLowerCase() shouldBe Schema(listOf(Column("a"), Column("b")))
      }
    }

    "Schema.contains" should {
      "return true if the schema contains the column" {
        schema.contains("a") shouldBe true
        schema.contains("b") shouldBe true
        schema.contains("C") shouldBe false
        schema.contains("A") shouldBe false
      }
    }

    "Schema" should {
      "return -1 if the column is not found" {
        val schema = Schema(listOf(
            Column("name", ColumnType.String, true, Precision(0), Scale(0), true),
            Column("age", ColumnType.Int, true, Precision(0), Scale(0), true),
            Column("salary", ColumnType.Double, true, Precision(0), Scale(0), true),
            Column("isPartTime", ColumnType.Boolean, true, Precision(0), Scale(0), true),
            Column("value1", ColumnType.Decimal, true, Precision(0), Scale(0), true),
            Column("value2", ColumnType.Float, true, Precision(0), Scale(0), true),
            Column("value3", ColumnType.Long, true, Precision(0), Scale(0), true)
        ))
        schema.indexOf("value4") shouldBe -1
      }

      "pretty print in desired format" {
        schema.show() shouldBe "- a [Boolean null scale=22 precision=0 signed]\n- b [String not null scale=0 precision=14 unsigned]"
      }
      //      "be inferred from the inner Person case class" in {
      //        Schema.from[Person] shouldBe {
      //          Schema(List(
      //              Column("name", ColumnType.String, true, Precision(0), Scale(0), true, None),
      //              Column("age", ColumnType.Int, true, Precision(0), Scale(0), true, None),
      //              Column("salary", ColumnType.Double, true, Precision(0), Scale(0), true, None),
      //              Column("isPartTime", ColumnType.Boolean, true, Precision(0), Scale(0), true, None),
      //              Column("value1", ColumnType.Decimal, true, Precision(0), Scale(0), true, None),
      //              Column("value2", ColumnType.Float, true, Precision(0), Scale(0), true, None),
      //              Column("value3", ColumnType.Long, true, Precision(0), Scale(0), true, None)
      //          ))
      //        }
      //      }
      //      "be inferred from the outer Person case class" in {
      //        Schema.from[Person] shouldBe {
      //          Schema(List(
      //              Column("name", ColumnType.String, true, Precision(0), Scale(0), true, None),
      //              Column("age", ColumnType.Int, true, Precision(0), Scale(0), true, None),
      //              Column("salary", ColumnType.Double, true, Precision(0), Scale(0), true, None),
      //              Column("isPartTime", ColumnType.Boolean, true, Precision(0), Scale(0), true, None),
      //              Column("value1", ColumnType.Decimal, true, Precision(0), Scale(0), true, None),
      //              Column("value2", ColumnType.Float, true, Precision(0), Scale(0), true, None),
      //              Column("value3", ColumnType.Long, true, Precision(0), Scale(0), true, None)
      //          ))
      //        }
      //      }
    }

    "Schema.updateColumnType" should {
      "set new schema type and leave other fields untouched" {
        Schema(
            Column("a", ColumnType.Int, true),
            Column("b", ColumnType.Short, false, scale = Scale(2), precision = Precision(3))
        ).updateColumnType("b", ColumnType.Boolean) shouldBe
            Schema(
                Column("a", ColumnType.Int, true, Precision(0), Scale(0), true),
                Column("b", ColumnType.Boolean, false, scale = Scale(2), precision = Precision(3))
            )
      }
    }
  }

  // data class Person(name: String, age: Int, salary: Double, isPartTime: Boolean, value1: BigDecimal, value2: Float, value3: Long)

}

//data class PersonOuter(name: String, age: Int, salary: Double, isPartTime: Boolean, value1: BigDecimal, value2: Float, value3: Long)