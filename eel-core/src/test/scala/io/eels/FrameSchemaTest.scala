package io.eels

import org.scalatest.{WordSpec, Matchers}

class FrameSchemaTest extends WordSpec with Matchers {

  val schema = FrameSchema(List(
    Column("a", SchemaType.Boolean, signed = true, scale = 22, nullable = true),
    Column("b", SchemaType.String, precision = 14, signed = false, nullable = false)
  ))

  "FrameSchema" should {
    "pretty print in desired format" in {
      schema.print shouldBe "- a [Boolean null scale=22 precision=0 signed]\n- b [String not null scale=0 precision=14 unsigned]"
    }
  }
}
