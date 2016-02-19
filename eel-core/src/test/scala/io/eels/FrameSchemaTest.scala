package io.eels

import org.scalatest.{WordSpec, Matchers}

class FrameSchemaTest extends WordSpec with Matchers {

  val schema = FrameSchema(List(Column("a"), Column("b")))

  "FrameSchema" should {
    "pretty print in desired format" in {
      schema.print shouldBe "- a [String]\n- b [String]"
    }
  }
}
