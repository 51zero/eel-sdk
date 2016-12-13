package io.eels

import io.eels.schema._
import org.scalatest.{Matchers, WordSpec}

class StructTypeTest extends WordSpec with Matchers {

  val schema = StructType(
    Field("a", BooleanType, nullable = true),
    Field("b", StringType, nullable = false)
  )

  "Schema.toLowerCase " should {
    " lower case all field names" in {
      StructType(Field("a"), Field("B")).toLowerCase() shouldBe StructType(Field("a"), Field("b"))
    }
  }

  "Schema.removeField " should {
    " remove the field from the schema" in {
      StructType(Field("a"), Field("B")).removeField("a") shouldBe StructType(Field("B"))
    }
  }
  "Schema.removeField " should {
    " remove the field from the schema ignoring case" in {
      StructType(Field("a"), Field("B")).removeField("A", caseSensitive = false) shouldBe StructType(Field("B"))
    }
  }

  "Schema.addField " should {
    " add new field to schema" in {
      StructType(Field("a")).addField("b") shouldBe StructType(Field("a"), Field("b"))
    }
  }
  "Schema.addFieldIfNotExists " should {
    " add new field to schema if not already present" in {
      StructType(Field("a")).addFieldIfNotExists("a") shouldBe StructType(Field("a"))
    }
  }

  "Schema.removeFields " should {
    " remove multiple fields if existing" in {
      StructType(Field("a"), Field("b"), Field("c")).removeFields("c", "a") shouldBe StructType(Field("b"))
    }
  }

  "Schema.size " should {
    " return number of fields" in {
      StructType(Field("a"), Field("b"), Field("c")).size() shouldBe 3
    }
  }
  "Schema.indexOf(name) " should {
    " return the index of the field zero indexed" in {
      StructType(Field("a"), Field("b"), Field("c")).indexOf("b") shouldBe 1
    }
  }

  "Schema.indexOf(name) " should {
    " return the index of the field zero indexed ignore case" in {
      StructType(Field("a"), Field("b"), Field("c")).indexOf("B", caseSensitive = false) shouldBe 1
    }
  }

  "Schema.contains(name) " should {
    " return true if the schema contains the field name" in {
      StructType(Field("a"), Field("b"), Field("c")).contains("B", caseSensitive = false) shouldBe true
    }
  }

  "Schema.replaceField" should {
    "replace the given field with the new field" in {
      StructType(Field("a"), Field("b"), Field("c")).replaceField("b", Field("d")) shouldBe
        StructType(Field("a"), Field("d"), Field("c"))
    }
  }

  "StructType.replaceDataType" should {
    "support decimal matches" in {
      val decimal34 = DecimalType(Precision(4), Scale(3))
      val decimal45 = DecimalType(Precision(5), Scale(4))
      StructType(Field("a", dataType = decimal45), Field("b", dataType = decimal34))
        .replaceFieldType(decimal34, StringType) shouldBe StructType(Field("a", dataType = decimal45), Field("b"))
    }
    "support decimal wildcards" in {
      val decimal34 = DecimalType(Precision(4), Scale(3))
      val decimal45 = DecimalType(Precision(5), Scale(4))
      StructType(Field("a", dataType = decimal45), Field("b", dataType = decimal34))
        .replaceFieldType(DecimalType.Wildcard, StringType) shouldBe StructType(Field("a"), Field("b"))
    }
    "support decimal part wildcards" in {
      val decimal34 = DecimalType(Precision(4), Scale(3))
      val decimal45 = DecimalType(Precision(5), Scale(4))
      StructType(Field("a", dataType = decimal45), Field("b", dataType = decimal34))
        .replaceFieldType(DecimalType(Precision(5), Scale(-1)), StringType) shouldBe StructType(Field("a"), Field("b", dataType = decimal34))
    }
  }

  "Schema.renameField " should {
    " update the field name" in {
      StructType(Field("a"), Field("b", dataType = DecimalType())).renameField("b", "d") shouldBe
        StructType(Field("a"), Field("d", dataType = DecimalType()))
    }
  }

  "Schema.contains(name) " should {
    " support structs" in {
      val b = Field("b", dataType = StructType(Seq(Field("d"))))
      StructType(Field("a"), b, Field("c")).contains("d") shouldBe true
      StructType(Field("a"), b, Field("c")).contains("e") shouldBe false
    }
  }

  "Schema.contains(name) " should {
    " support structs ignore case" in {
      val b = Field("b", dataType = StructType(Seq(Field("d"))))
      StructType(Field("a"), b, Field("c")).contains("D", false) shouldBe true
      StructType(Field("a"), b, Field("c")).contains("E", false) shouldBe false
    }
  }

  "Schema.contains " should {
    " return true if the schema contains the column" in {
      schema.contains("a") shouldBe true
      schema.contains("b") shouldBe true
      schema.contains("C") shouldBe false
      schema.contains("A") shouldBe false
    }
  }
  "Schema.indexOf" should {
    "return -1 if the column is not found" in {
      val schema = StructType(
        Field("name", StringType, true),
        Field("age", IntType(true), true),
        Field("salary", DoubleType, true),
        Field("isPartTime", BooleanType, true),
        Field("value2", FloatType, true),
        Field("value3", LongType(true), true)
      )
      schema.indexOf("aaaa") shouldBe -1
    }
  }

  "Schema.show" should {
    "pretty print in desired format" in {
      //     schema.show() shouldBe "- a [Boolean null scale=22 precision=0 signed]\n- b [String not null scale=0 precision=14 unsigned]"
    }
  }

  "Schema.from[T]" should {
    "be inferred from the type" in {
      StructType.from[Person] shouldBe {
        StructType(List(
          Field("name", StringType, true),
          Field("age", IntType(true), true),
          Field("salary", DoubleType, true),
          Field("isPartTime", BooleanType, true),
          Field("value1", DecimalType(Precision(18), Scale(2)), true),
          Field("value2", FloatType, true),
          Field("value3", LongType(true), true)
        ))
      }
    }
  }

  "Schema.updateFieldType " should {
    " set new schema type and leave other fields untouched" in {
      StructType(
        Field("a", IntType(true), true),
        Field("b", ShortType.Signed)
      ).updateFieldType("b", BooleanType) shouldBe
        StructType(
          Field("a", IntType(true)),
          Field("b", BooleanType)
        )
    }
  }
}

case class Person(name: String, age: Int, salary: Double, isPartTime: Boolean, value1: BigDecimal, value2: Float, value3: Long)
