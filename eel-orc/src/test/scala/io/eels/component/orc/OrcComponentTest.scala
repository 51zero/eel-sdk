package io.eels.component.orc

import java.io.{File, FilenameFilter}
import java.sql.Timestamp

import com.sun.javafx.PlatformUtil
import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.orc.TypeDescription
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

class OrcComponentTest extends WordSpec with Matchers with BeforeAndAfter {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.get(conf)

  cleanUpResidualOrcTestFiles

  "OrcComponent" should {
    "read and write orc files for all supported types" in {

      val desc = TypeDescription.createStruct()
        .addField("maps", TypeDescription.createMap(TypeDescription.createString, TypeDescription.createBoolean))
        .addField("lists", TypeDescription.createList(TypeDescription.createString))
      desc.createRowBatch(1000)

      val schema = StructType(
        Field("string", StringType),
        Field("char", CharType(2)),
        Field("int", IntType.Signed),
        Field("double", DoubleType),
        Field("boolean", BooleanType),
        Field("long", LongType.Signed),
        Field("decimal", DecimalType(4, 2)),
        Field("timestamp", TimestampMillisType),
        Field("varchar", VarcharType(100)),
        Field("list", ArrayType(StringType)),
        Field("map", MapType(StringType, BooleanType))
      )

      desc.createRowBatch(3332)

      val ds = DataStream.fromRows(
        schema,
        Row(schema, Vector("hello", "aa", 85, 1.9, true, 3256269123123L, 9.91, 1483726491000L, "abcdef", Seq("x", "y", "z"), Map("a" -> true, "b" -> false))),
        Row(schema, Vector("world", "bb", 65, 1.7, true, 1950173241323L, 3.9, 1483726291000L, "qwerty", Seq("p", "q", "r"), Map("x" -> false, "y" -> true)))
      )
      val path = new Path(s"test_${System.currentTimeMillis()}.orc")
      fs.delete(path, false)
      ds.to(OrcSink(path))

      val rows = OrcSource(path).toDataStream().toSet
      rows.size shouldBe 2
      fs.delete(path, false)

      rows.head.schema shouldBe ds.schema

      rows shouldBe Set(
        Row(schema, Vector("hello", "aa", 85, 1.9, true, 3256269123123L, BigDecimal(9.91), new Timestamp(1483726491000L), "abcdef", Seq("x", "y", "z"), Map("a" -> true, "b" -> false))),
        Row(schema, Vector("world", "bb", 65, 1.7, true, 1950173241323L, BigDecimal(3.9), new Timestamp(1483726291000L), "qwerty", Seq("p", "q", "r"), Map("x" -> false, "y" -> true)))
      )
    }
    "handle null values" in {
      val schema = StructType(
        Field("a", StringType),
        Field("b", StringType, true),
        Field("c", DateType, true)
      )

      val ds = DataStream.fromRows(
        schema,
        Row(schema, Vector("a1", null, null)),
        Row(schema, Vector("a2", "b2", null))
      )

      val path = new Path(s"test_${System.currentTimeMillis()}.orc")
      ds.to(OrcSink(path))

      val rows = OrcSource(path).toDataStream().toSet
      rows.size shouldBe 2
      rows.head.schema shouldBe ds.schema
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

      val ds = DataStream.fromRows(
        schema,
        Row(schema, Vector("a1", Vector("c1"))),
        Row(schema, Vector("a2", Vector("c2")))
      )

      val path = new Path(s"test_${System.currentTimeMillis()}.orc")
      ds.to(OrcSink(path))

      val rows = OrcSource(path).toDataStream().toSet
      rows.size shouldBe 2
      rows.head.schema shouldBe ds.schema
      rows shouldBe Set(
        Row(schema, Vector("a1", Vector("c1"))),
        Row(schema, Vector("a2", Vector("c2")))
      )
    }
    "support projections" in {
      val schema = StructType(Field("a", StringType), Field("b", BooleanType), Field("c", IntType.Signed))
      val projectedSchema = schema.removeField("b")

      val ds = DataStream.fromRows(schema,
        Row(schema, Vector("x", true, 1)),
        Row(schema, Vector("y", false, 2))
      )
      val path = new Path(s"test_${System.currentTimeMillis()}.orc")
      ds.to(OrcSink(path))

      val rows = OrcSource(path).withProjection("a", "c").toDataStream().toSet
      rows.size shouldBe 2
      rows.head.schema shouldBe projectedSchema
      rows shouldBe Set(
        Row(projectedSchema, Vector("x", 1)),
        Row(projectedSchema, Vector("y", 2))
      )
    }
    "support overwrite option" in {

      val schema = StructType(Field("a", StringType))
      val frame = DataStream.fromRows(schema,
        Row(schema, Vector("x")),
        Row(schema, Vector("y"))
      )

      val path = new Path("overwrite_test.orc")
      frame.to(OrcSink(path))
      frame.to(OrcSink(path).withOverwrite(true))
      fs.delete(path, false)
    }
    "support permissions" in {

      val path = new Path("permissions.pq")

      val schema = StructType(Field("a", StringType))
      val ds = DataStream.fromRows(schema,
        Row(schema, Vector("x")),
        Row(schema, Vector("y"))
      )

      ds.to(OrcSink(path).withOverwrite(true).withPermission(FsPermission.valueOf("-rw-r----x")))
      if (!PlatformUtil.isWindows) fs.getFileStatus(path).getPermission.toString shouldBe "rw-r----x"
      fs.delete(path, false)
    }
  }

  private def cleanUpResidualOrcTestFiles = {
    new File(".").listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        (name.startsWith("test_") && name.endsWith(".orc")) || (name.startsWith(".test_") && name.endsWith(".orc.crc"))
      }
    }).foreach(_.delete())
  }
}