package io.eels.component.sequence

import io.eels.Row
import io.eels.schema.{Field, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class SequenceSourceTest extends WordSpec with Matchers {

  implicit val conf = new Configuration()

  val schema = StructType(Field("name"), Field("location"))
  val frame = io.eels.Frame.fromValues(
    schema,
    Vector("name", "location"),
    Vector("sam", "aylesbury"),
    Vector("jam", "aylesbury"),
    Vector("ham", "buckingham")
  )

  "SequenceSource" should {
    "read sequence files" in {
      val schema = StructType(
        Field("a", StringType),
        Field("b", StringType),
        Field("c", StringType),
        Field("d", StringType)
      )
      val path = new Path(getClass.getResource("/test.seq").getFile)
      val rows = SequenceSource(path).toFrame().toSet
      rows shouldBe Set(
        Row(schema, "1", "2", "3", "4"),
        Row(schema, "5", "6", "7", "8")
      )
    }
    "read header as schema" in {
      val path = new Path(getClass.getResource("/test.seq").getFile)
      SequenceSource(path).schema() shouldBe StructType(
        Field("a", StringType),
        Field("b", StringType),
        Field("c", StringType),
        Field("d", StringType)
      )
    }
  }
}