package io.eels

import org.scalatest.{WordSpec, Matchers}

class FrameSchemaTest extends WordSpec with Matchers {

  val columns = Seq(Column("a"), Column("b"))
  val frame = Frame(Row(columns, Seq("1", "2")), Row(columns, Seq("3", "4")))

  "FrameSchema" should {
    "pretty print in desired format" in {
      frame.schema.print shouldBe "- a [String]\n- b [String]"
    }
  }
}
