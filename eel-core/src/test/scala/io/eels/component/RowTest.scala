package io.eels.component

import io.eels.{SchemaType, Row, Frame, Column}
import org.scalatest.{WordSpec, Matchers}

class RowTest extends WordSpec with Matchers {

  "Row.removeColumn" should {
    "preserve column schema" in {
      val columns = List(
        Column("a", `type` = SchemaType.Int, nullable = true),
        Column("b", `type` = SchemaType.Double, nullable = true)
      )
      val row = Row(columns, List("1", "2"))
      row.removeColumn("a") shouldBe Row(
        List(Column("b", `type` = SchemaType.Double, nullable = true)),
        List("2")
      )
    }
  }
}
