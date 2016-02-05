package io.eels.component.sequence

import com.sksamuel.scalax.io.IO
import io.eels.{SchemaType, FrameSchema, Column, Frame, Row}
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class SequenceSourceTest extends WordSpec with Matchers {

  val columns = List(Column("a"), Column("b"), Column("c"), Column("d"))
  val frame = Frame(Row(columns, List("1", "2", "3", "4")), Row(columns, List("5", "6", "7", "8")))

  "SequenceSource" should {
    "read sequence files" in {
      val path = new Path(IO.fileFromResource("/test.seq").getAbsolutePath)
      val rows = SequenceSource(path).toList
      rows shouldBe List(Row(columns, List("1", "2", "3", "4")), Row(columns, List("5", "6", "7", "8")))
    }
    "read header as schema" in {
      val path = new Path(IO.fileFromResource("/test.seq").getAbsolutePath)
      SequenceSource(path).schema shouldBe FrameSchema(
        List(
          Column("a", SchemaType.String, false),
          Column("b", SchemaType.String, false),
          Column("c", SchemaType.String, false),
          Column("d", SchemaType.String, false)
        )
      )
    }
  }
}
