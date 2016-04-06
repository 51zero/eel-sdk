package io.eels

import io.kotlintest.specs.WordSpec

class SchemaTest : WordSpec() {

  val schema = Schema(listOf(
      Column("a", ColumnType.Boolean, signed = true, scale = 22, nullable = true),
      Column("b", ColumnType.String, precision = 14, signed = false, nullable = false)
  ))

  init {

    "Schema.toLowerCase" should {
      "lower case all column names" with {
        Schema(listOf(Column("a"), Column("B"))).toLowerCase() shouldBe Schema(listOf(Column("a"), Column("b")))
      }
    }

    "Schema.contains" should {
      "return true if the schema contains the column" with {
        schema.contains("a") shouldBe true
        schema.contains("b") shouldBe true
        schema.contains("C") shouldBe false
        schema.contains("A") shouldBe false
      }
    }

    "Schema" should {
      "return -1 if the column is not found" with {
        val schema = Schema(listOf(
            Column("name", ColumnType.String, true, 0, 0, true),
            Column("age", ColumnType.Int, true, 0, 0, true),
            Column("salary", ColumnType.Double, true, 0, 0, true),
            Column("isPartTime", ColumnType.Boolean, true, 0, 0, true),
            Column("value1", ColumnType.Decimal, true, 0, 0, true),
            Column("value2", ColumnType.Float, true, 0, 0, true),
            Column("value3", ColumnType.Long, true, 0, 0, true)
        ))
        schema.indexOf("value4") shouldBe -1
      }

      "pretty print in desired format" with {
        schema.print() shouldBe "- a [Boolean null scale=22 precision=0 signed]\n- b [String not null scale=0 precision=14 unsigned]"
      }
      //      "be inferred from the inner Person case class" in {
      //        Schema.from[Person] shouldBe {
      //          Schema(List(
      //              Column("name", ColumnType.String, true, 0, 0, true, None),
      //              Column("age", ColumnType.Int, true, 0, 0, true, None),
      //              Column("salary", ColumnType.Double, true, 0, 0, true, None),
      //              Column("isPartTime", ColumnType.Boolean, true, 0, 0, true, None),
      //              Column("value1", ColumnType.Decimal, true, 0, 0, true, None),
      //              Column("value2", ColumnType.Float, true, 0, 0, true, None),
      //              Column("value3", ColumnType.Long, true, 0, 0, true, None)
      //          ))
      //        }
      //      }
      //      "be inferred from the outer Person case class" in {
      //        Schema.from[Person] shouldBe {
      //          Schema(List(
      //              Column("name", ColumnType.String, true, 0, 0, true, None),
      //              Column("age", ColumnType.Int, true, 0, 0, true, None),
      //              Column("salary", ColumnType.Double, true, 0, 0, true, None),
      //              Column("isPartTime", ColumnType.Boolean, true, 0, 0, true, None),
      //              Column("value1", ColumnType.Decimal, true, 0, 0, true, None),
      //              Column("value2", ColumnType.Float, true, 0, 0, true, None),
      //              Column("value3", ColumnType.Long, true, 0, 0, true, None)
      //          ))
      //        }
      //      }
    }

    "Schema.updateColumnType" should {
      "set new schema type and leave other fields untouched" with {
        Schema(
            Column("a", ColumnType.Int, true),
            Column("b", ColumnType.Short, false, scale = 2, precision = 3)
        ).updateColumnType("b", ColumnType.Boolean) shouldBe
            Schema(
                Column("a", ColumnType.Int, true, 0, 0, true),
                Column("b", ColumnType.Boolean, false, scale = 2, precision = 3)
            )
      }
    }
  }

  // data class Person(name: String, age: Int, salary: Double, isPartTime: Boolean, value1: BigDecimal, value2: Float, value3: Long)

}

//data class PersonOuter(name: String, age: Int, salary: Double, isPartTime: Boolean, value1: BigDecimal, value2: Float, value3: Long)