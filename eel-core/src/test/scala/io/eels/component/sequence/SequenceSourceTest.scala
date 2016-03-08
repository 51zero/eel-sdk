package io.eels.component.sequence

import com.sksamuel.scalax.io.IO
import io.eels._
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class SequenceSourceTest extends WordSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  val frame = Frame(
    Vector("name", "location"),
    Vector("sam", "aylesbury"),
    Vector("jam", "aylesbury"),
    Vector("ham", "buckingham")
  )

  "SequenceSource" should {
    "read sequence files" in {
      val schema = Schema(List(
        Column("a", SchemaType.String, false, 0, 0, true, None),
        Column("b", SchemaType.String, false, 0, 0, true, None),
        Column("c", SchemaType.String, false, 0, 0, true, None),
        Column("d", SchemaType.String, false, 0, 0, true, None)
      ))
      val path = new Path(IO.fileFromResource("/test.seq").getAbsolutePath)
      val rows = SequenceSource(path).toSet
      rows shouldBe Set(
        Row(schema, "1", "2", "3", "4"),
        Row(schema, "5", "6", "7", "8")
      )
    }
    "read header as schema" in {
      val path = new Path(IO.fileFromResource("/test.seq").getAbsolutePath)
      SequenceSource(path).schema shouldBe Schema(
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
