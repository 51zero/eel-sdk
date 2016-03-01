package io.eels

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalatest.{WordSpec, Matchers}

class SchemaTest extends WordSpec with Matchers {

  val schema = Schema(List(
    Column("a", SchemaType.Boolean, signed = true, scale = 22, nullable = true),
    Column("b", SchemaType.String, precision = 14, signed = false, nullable = false)
  ))

  "Schema" should {
    "pretty print in desired format" in {
      schema.print shouldBe "- a [Boolean null scale=22 precision=0 signed]\n- b [String not null scale=0 precision=14 unsigned]"
    }
    "be inferred from the inner Person case class" in {
      Schema.from[Person] shouldBe {
        Schema(List(
          Column("name", SchemaType.String, true, 0, 0, true, None),
          Column("age", SchemaType.Int, true, 0, 0, true, None),
          Column("salary", SchemaType.Double, true, 0, 0, true, None),
          Column("isPartTime", SchemaType.Boolean, true, 0, 0, true, None),
          Column("value1", SchemaType.Decimal, true, 0, 0, true, None),
          Column("value2", SchemaType.Float, true, 0, 0, true, None),
          Column("value3", SchemaType.Long, true, 0, 0, true, None)
        ))
      }
    }
    "be inferred from the outer Person case class" in {
      Schema.from[Person] shouldBe {
        Schema(List(
          Column("name", SchemaType.String, true, 0, 0, true, None),
          Column("age", SchemaType.Int, true, 0, 0, true, None),
          Column("salary", SchemaType.Double, true, 0, 0, true, None),
          Column("isPartTime", SchemaType.Boolean, true, 0, 0, true, None),
          Column("value1", SchemaType.Decimal, true, 0, 0, true, None),
          Column("value2", SchemaType.Float, true, 0, 0, true, None),
          Column("value3", SchemaType.Long, true, 0, 0, true, None)
        ))
      }
    }
  }

  "Schema.updateSchemaType" should {
    "set new schema type and leave other fields untouched" in {
      Schema(
        Column("a", SchemaType.Int, true),
        Column("b", SchemaType.Short, false, scale = 2, precision = 3)
      ).updateSchemaType("b", SchemaType.Boolean) shouldBe
        Schema(
          Column("a", SchemaType.Int, true, 0, 0, true, None),
          Column("b", SchemaType.Boolean, false, scale = 2, precision = 3)
        )
    }
  }

  case class Person(name: String, age: Int, salary: Double, isPartTime: Boolean, value1: BigDecimal, value2: Float, value3: Long) extends StrictLogging

}

case class PersonOuter(name: String, age: Int, salary: Double, isPartTime: Boolean, value1: BigDecimal, value2: Float, value3: Long) extends StrictLogging