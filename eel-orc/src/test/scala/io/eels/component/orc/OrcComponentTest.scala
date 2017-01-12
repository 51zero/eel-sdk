package io.eels.component.orc

import java.sql.Timestamp

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

  val path = new Path("test.orc")

  "OrcComponent" should {
    "read and write orc files" in {

      val schema = StructType(
        Field("string", StringType),
        Field("int", IntType.Signed),
        Field("double", DoubleType),
        Field("boolean", BooleanType),
        Field("long", LongType.Signed),
        Field("decimal", DecimalType(4, 2)),
        Field("timestamp", TimestampMillisType)
      )

      val frame = Frame(
        schema,
        Row(schema, Vector("a", 85, 1.9, true, 3256269123123L, 9.91, 1483726491000L)),
        Row(schema, Vector("b", 65, 1.7, true, 1950173241323L, 3.9, 1483726291000L))
      )

      fs.delete(path, false)
      frame.to(OrcSink(path))

      val rows = OrcSource(path).toFrame().toSet()
      rows.size shouldBe 2
      fs.delete(path, false)

      rows.head.schema shouldBe frame.schema

      rows shouldBe Set(
        Row(schema, Vector("a", 85, 1.9, true, 3256269123123L, 9.91, new Timestamp(1483726491000L))),
        Row(schema, Vector("b", 65, 1.7, true, 1950173241323L, 3.9, new Timestamp(1483726291000L)))
      )

      fs.delete(path, false)
    }
    "handle null values" in {
      fs.delete(path, false)

      val schema = StructType(
        Field("a", StringType),
        Field("b", StringType, true),
        Field("c", DateType, true)
      )

      val frame = Frame(
        schema,
        Row(schema, Vector("a1", null, null)),
        Row(schema, Vector("a2", "b2", null))
      )

      frame.to(OrcSink(path))

      val rows = OrcSource(path).toFrame().toSet()
      rows.size shouldBe 2
      rows.head.schema shouldBe frame.schema
      rows shouldBe Set(
        Row(schema, Vector("a1", null, null)),
        Row(schema, Vector("a2", "b2", null))
      )

      fs.delete(path, false)
    }
  }
}