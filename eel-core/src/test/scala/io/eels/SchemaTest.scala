package io.eels

import org.scalatest.{Matchers, WordSpec}

class SchemaTest extends WordSpec with Matchers {

  "Schema.updateSchemaType" should {
    "set new schema type and leave other fields untouched" in {
      FrameSchema(
        Column("a", SchemaType.Int, true),
        Column("b", SchemaType.Short, false, scale = 2, precision = 3)
      ).updateSchemaType("b", SchemaType.Boolean) shouldBe
        FrameSchema(
          Column("a", SchemaType.Int, true, 0, 0, true, None),
          Column("b", SchemaType.Boolean, false, scale = 2, precision = 3)
        )
    }
  }
}
