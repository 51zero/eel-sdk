package io.eels

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Precision
import io.eels.schema.Scale
import io.eels.schema.Schema
import io.kotlintest.specs.StringSpec

class SchemaTest : StringSpec() {

  val schema = Schema(listOf(
      Field("a", FieldType.Boolean, signed = true, scale = Scale(22), nullable = true),
      Field("b", FieldType.String, precision = Precision(14), signed = false, nullable = false)
  ))

  init {

    "Schema.toLowerCase should lower case all field names"{
      Schema(listOf(Field("a"), Field("B"))).toLowerCase() shouldBe Schema(listOf(Field("a"), Field("b")))
    }

    "Schema.removeField should remove the field from the schema" {
      Schema(listOf(Field("a"), Field("B"))).removeField("a") shouldBe Schema(listOf(Field("B")))
    }

    "Schema.removeField should remove the field from the schema ignoring case" {
      Schema(listOf(Field("a"), Field("B"))).removeField("A", caseSensitive = false) shouldBe Schema(listOf(Field("B")))
    }

    "Schema.addField should add new field to schema" {
      Schema(listOf(Field("a"))).addField("b") shouldBe Schema(listOf(Field("a"), Field("b")))
    }

    "Schema.addFieldIfNotExists should add new field to schema if not already present" {
      Schema(listOf(Field("a"))).addFieldIfNotExists("a") shouldBe Schema(listOf(Field("a")))
    }

    "Schema.removeFields should remove multiple fields if existing" {
      Schema(Field("a"), Field("b"), Field("c")).removeFields("c", "a") shouldBe Schema(Field("b"))
    }

    "Schema.size should return number of fields" {
      Schema(Field("a"), Field("b"), Field("c")).size() shouldBe 3
    }

    "Schema.indexOf(name) should return the index of the field zero indexed" {
      Schema(Field("a"), Field("b"), Field("c")).indexOf("b") shouldBe 1
    }

    "Schema.indexOf(name) should return the index of the field zero indexed ignore case" {
      Schema(Field("a"), Field("b"), Field("c")).indexOf("B", caseSensitive = false) shouldBe 1
    }

    "Schema.contains(name) should return true if the schema contains the field name" {
      Schema(Field("a"), Field("b"), Field("c")).contains("B", caseSensitive = false) shouldBe true
    }

    "Schema.replaceField should replace the given field with the new field" {
      Schema(Field("a"), Field("b"), Field("c")).replaceField("b", Field("d")) shouldBe Schema(Field("a"), Field("d"), Field("c"))
    }

    "Schema.renameField should update the field name" {
      Schema(Field("a"), Field("b", type = FieldType.Decimal)).renameField("b", "d") shouldBe Schema(Field("a"), Field("d", type = FieldType.Decimal))
    }

    "Schema.contains(name) should support structs" {
      val b = Field("b", type = FieldType.Struct, fields = listOf(Field("d")))
      Schema(Field("a"), b, Field("c")).contains("d") shouldBe true
      Schema(Field("a"), b, Field("c")).contains("e") shouldBe false
    }

    "Schema.contains(name) should support structs ignore case" {
      val b = Field("b", type = FieldType.Struct, fields = listOf(Field("d")))
      Schema(Field("a"), b, Field("c")).contains("D", false) shouldBe true
      Schema(Field("a"), b, Field("c")).contains("E", false) shouldBe false
    }

    "Schema.contains should return true if the schema contains the column" {
      schema.contains("a") shouldBe true
      schema.contains("b") shouldBe true
      schema.contains("C") shouldBe false
      schema.contains("A") shouldBe false
    }

    "should return -1 if the column is not found"  {
      val schema = Schema(listOf(
          Field("name", FieldType.String, true, Precision(0), Scale(0), true),
          Field("age", FieldType.Int, true, Precision(0), Scale(0), true),
          Field("salary", FieldType.Double, true, Precision(0), Scale(0), true),
          Field("isPartTime", FieldType.Boolean, true, Precision(0), Scale(0), true),
          Field("value1", FieldType.Decimal, true, Precision(0), Scale(0), true),
          Field("value2", FieldType.Float, true, Precision(0), Scale(0), true),
          Field("value3", FieldType.Long, true, Precision(0), Scale(0), true)
      ))
      schema.indexOf("value4") shouldBe -1
    }

    "pretty print in desired format" {
      //     schema.show() shouldBe "- a [Boolean null scale=22 precision=0 signed]\n- b [String not null scale=0 precision=14 unsigned]"
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

    "Schema.updateFieldType should set new schema type and leave other fields untouched"  {
      Schema(
          Field("a", FieldType.Int, true),
          Field("b", FieldType.Short, false, scale = Scale(2), precision = Precision(3))
      ).updateFieldType("b", FieldType.Boolean) shouldBe
          Schema(
              Field("a", FieldType.Int, true, Precision(0), Scale(0), false),
              Field("b", FieldType.Boolean, false, scale = Scale(2), precision = Precision(3))
          )
    }
  }
}

// data class Person(name: String, age: Int, salary: Double, isPartTime: Boolean, value1: BigDecimal, value2: Float, value3: Long)

//data class PersonOuter(name: String, age: Int, salary: Double, isPartTime: Boolean, value1: BigDecimal, value2: Float, value3: Long)