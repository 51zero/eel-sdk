package io.eels.component

import io.eels.{Column, Row, SchemaType}
import org.scalatest.{Matchers, WordSpec}

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
    "allow renaming of columns" in {
      val columns = List(
        Column("a", `type` = SchemaType.Int, nullable = true),
        Column("b", `type` = SchemaType.Double, nullable = true)
      )
      val row = Row(columns, List("1", "2"))
      row.renameColumn("a", "c") shouldBe
        Row(
          List(
            Column("c", `type` = SchemaType.Int, nullable = true),
            Column("b", `type` = SchemaType.Double, nullable = true)
          ),
          List("1", "2")
        )
    }
  }
}
