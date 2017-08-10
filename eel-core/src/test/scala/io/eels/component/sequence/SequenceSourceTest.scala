package io.eels.component.sequence

import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema.{Field, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class SequenceSourceTest extends WordSpec with Matchers {

  private implicit val conf = new Configuration()

  private val schema = StructType(Field("name"), Field("location"))
  private val ds = DataStream.fromValues(
    schema,
    Seq(
      Vector("name", "location"),
      Vector("sam", "aylesbury"),
      Vector("jam", "aylesbury"),
      Vector("ham", "buckingham")
    )
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
      val rows = SequenceSource(path).toDataStream().toSet
      rows shouldBe Set(
        Vector("1", "2", "3", "4"),
        Vector("5", "6", "7", "8")
      )
    }
    "read header as schema" in {
      val path = new Path(getClass.getResource("/test.seq").getFile)
      SequenceSource(path).schema shouldBe StructType(
        Field("a", StringType),
        Field("b", StringType),
        Field("c", StringType),
        Field("d", StringType)
      )
    }
  }
}