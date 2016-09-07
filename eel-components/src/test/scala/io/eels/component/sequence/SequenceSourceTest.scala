package io.eels.component.sequence

import io.eels.Row
import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class SequenceSourceTest extends WordSpec with Matchers {

  implicit val conf = new Configuration()

  val schema = Schema(Field("name"), Field("location"))
  val frame = io.eels.Frame.fromValues(
    schema,
    Vector("name", "location"),
    Vector("sam", "aylesbury"),
    Vector("jam", "aylesbury"),
    Vector("ham", "buckingham")
  )

  "SequenceSource" should {
    "read sequence files" in {
      val schema = Schema(
        Field("a", FieldType.String),
        Field("b", FieldType.String),
        Field("c", FieldType.String),
        Field("d", FieldType.String)
      )
      val path = new Path(getClass.getResource("/test.seq").getFile)
      val rows = SequenceSource(path).toFrame(1).toSet
      rows shouldBe Set(
        Row(schema, "1", "2", "3", "4"),
        Row(schema, "5", "6", "7", "8")
      )
    }
    "read header as schema" in {
      val path = new Path(getClass.getResource("/test.seq").getFile)
      SequenceSource(path).schema() shouldBe Schema(
        Field("a", FieldType.String),
        Field("b", FieldType.String),
        Field("c", FieldType.String),
        Field("d", FieldType.String)
      )
    }
  }
}