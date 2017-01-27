package io.eels.component.orc

import java.sql.Timestamp

import io.eels.{Frame, Row}
import io.eels.schema._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

class OrcComponentTest extends WordSpec with Matchers with BeforeAndAfter {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.get(conf)

  val path = new Path("test.orc")

  before {
    fs.delete(path, false)
  }

  after {
    fs.delete(path, false)
  }

  "OrcComponent" should {
    "read and write orc files for all supported types" in {

      val schema = StructType(
        Field("string", StringType),
        Field("char", CharType(2)),
        Field("int", IntType.Signed),
        Field("double", DoubleType),
        Field("boolean", BooleanType),
        Field("long", LongType.Signed),
        Field("decimal", DecimalType(4, 2)),
        Field("timestamp", TimestampMillisType),
        Field("varchar", VarcharType(100))
      )

      val frame = Frame(
        schema,
        Row(schema, Vector("hello", "aa", 85, 1.9, true, 3256269123123L, 9.91, 1483726491000L, "abcdef")),
        Row(schema, Vector("world", "bb", 65, 1.7, true, 1950173241323L, 3.9, 1483726291000L, "qwerty"))
      )

      fs.delete(path, false)
      frame.to(OrcSink(path))

      val rows = OrcSource(path).toFrame().toSet()
      rows.size shouldBe 2
      fs.delete(path, false)

      rows.head.schema shouldBe frame.schema

      rows shouldBe Set(
        Row(schema, Vector("hello", "aa", 85, 1.9, true, 3256269123123L, 9.91, new Timestamp(1483726491000L), "abcdef")),
        Row(schema, Vector("world", "bb", 65, 1.7, true, 1950173241323L, 3.9, new Timestamp(1483726291000L), "qwerty"))
      )
    }
    "handle null values" in {
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
    }
    "support structs" in {
      val schema = StructType(
        Field("a", StringType),
        Field("b", StructType(
          Field("c", StringType)
        ))
      )

      val frame = Frame(
        schema,
        Row(schema, Vector("a1", Vector("c1"))),
        Row(schema, Vector("a2", Vector("c2")))
      )

      frame.to(OrcSink(path))

      val rows = OrcSource(path).toFrame().toSet()
      rows.size shouldBe 2
      rows.head.schema shouldBe frame.schema
      rows shouldBe Set(
        Row(schema, Vector("a1", Vector("c1"))),
        Row(schema, Vector("a2", Vector("c2")))
      )
    }
    "support projections" in {
      val schema = StructType(Field("a", StringType), Field("b", BooleanType), Field("c", IntType.Signed))
      val projectedSchema = schema.removeField("b")

      val frame = Frame(schema,
        Row(schema, Vector("x", true, 1)),
        Row(schema, Vector("y", false, 2))
      )
      frame.to(OrcSink(path))

      val rows = OrcSource(path).withProjection("a", "c").toFrame().toSet
      rows.size shouldBe 2
      rows.head.schema shouldBe projectedSchema
      rows shouldBe Set(
        Row(projectedSchema, Vector("x", 1)),
        Row(projectedSchema, Vector("y", 2))
      )
    }
  }
}