package io.eels.component.sequence

import com.sksamuel.scalax.io.IO
import io.eels.{Column, Frame, FrameSchema, SchemaType}
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class SequenceSourceTest extends WordSpec with Matchers {

  val frame = Frame(
    List("name", "location"),
    List("sam", "aylesbury"),
    List("jam", "aylesbury"),
    List("ham", "buckingham")
  )

  "SequenceSource" should {
    "read sequence files" in {
      val path = new Path(IO.fileFromResource("/test.seq").getAbsolutePath)
      val rows = SequenceSource(path).toSeq.run
      rows shouldBe List(
        List("1", "2", "3", "4"),
        List("5", "6", "7", "8")
      )
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
