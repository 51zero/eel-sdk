package io.eels.component.sequence

import io.eels.Row
import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import org.apache.hadoop.fs.Path

class SequenceSourceTest extends WordSpec with Matchers {

  val schema = Schema(Field("name"), Field("location"))
  val frame = io.eels.Frame(
    schema,
      Vector("name", "location"),
      Vector("sam", "aylesbury"),
      Vector("jam", "aylesbury"),
      Vector("ham", "buckingham")
  )

  init {
    "SequenceSource" should {
      "read sequence files" {
        val schema = Schema(listOf(
          Field("a", FieldType.String),
          Field("b", FieldType.String),
          Field("c", FieldType.String),
          Field("d", FieldType.String)
        ))
        val path = Path(javaClass.getResource("/test.seq").file)
        val rows = SequenceSource(path).toFrame(1).toSet()
        rows shouldBe setOf(
          Row(schema, "1", "2", "3", "4"),
          Row(schema, "5", "6", "7", "8")
        )
      }
      "read header as schema" {
        val path = Path(javaClass.getResource("/test.seq").file)
        SequenceSource(path).schema() shouldBe Schema(
          listOf(
            Field("a", FieldType.String),
            Field("b", FieldType.String),
            Field("c", FieldType.String),
            Field("d", FieldType.String)
          )
        )
      }
    }
  }
}