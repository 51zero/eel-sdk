package io.eels.component.orc

import io.eels.Frame
import io.eels.Row
import io.eels.schema.Field
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class OrcComponentTest extends WordSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.get(conf)

  "OrcComponent" should {
    "read and write orc files" in {

      val schema = StructType(Field("name"), Field("job"), Field("location"))
      val frame = Frame(
        schema,
        Row(schema, Vector("clint eastwood", "actor", "carmel")),
        Row(schema, Vector("david bowie", "musician", "surrey"))
      )

      val path = new Path("test.orc")
      fs.delete(path, false)
      frame.to(OrcSink(path))

      val rows = OrcSource(path).toFrame().toSet()
      rows.size shouldBe 2
      fs.delete(path, false)

      rows.head.schema shouldBe frame.schema

      rows shouldBe Set(
        Row(frame.schema, Vector("clint eastwood", "actor", "carmel")),
        Row(frame.schema, Vector("david bowie", "musician", "surrey"))
      )
    }
  }
}