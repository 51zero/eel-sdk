package io.eels.component.orc

import io.eels.Frame
import io.eels.Row
import io.eels.schema._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class OrcComponentTest extends WordSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.get(conf)

  "OrcComponent" should {
    "read and write orc files" in {

      val schema = StructType(
        Field("name", StringType),
        Field("age", IntType.Signed),
        Field("height", DoubleType),
        Field("amazing", BooleanType),
        Field("fans", LongType.Signed),
        Field("rating", DecimalType(4, 2))
      )

      val frame = Frame(
        schema,
        Row(schema, Vector("clint eastwood", 85, 1.9, true, 3256269123123L, 9.99)),
        Row(schema, Vector("david bowie", 65, 1.7, true, 1950173241323L, 9.9))
      )

      val path = new Path("test.orc")
      fs.delete(path, false)
      frame.to(OrcSink(path))

      val rows = OrcSource(path).toFrame().toSet()
      rows.size shouldBe 2
      fs.delete(path, false)

      rows.head.schema shouldBe frame.schema

      rows shouldBe Set(
        Row(schema, Vector("clint eastwood", 85, 1.9, true, 3256269123123L, 9.99)),
        Row(schema, Vector("david bowie", 65, 1.7, true, 1950173241323L, 9.9))
      )

      fs.delete(path, false)
    }
  }
}