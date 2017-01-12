package io.eels.component.parquet

import java.io.File
import java.nio.file.Paths

import io.eels.schema._
import io.eels.{Frame, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class ParquetComponentTest extends WordSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.get(new Configuration())

  "Parquet" should {
    "write and read parquet files" in {

      val path = new Path("test.pq")
      if (fs.exists(path))
        fs.delete(path, false)

      val structType = StructType(
        Field("name", StringType, nullable = false),
        Field("job", StringType, nullable = false),
        Field("location", StringType, nullable = false)
      )

      val frame = Frame.fromValues(
        structType,
        Vector("clint eastwood", "actor", "carmel"),
        Vector("elton john", "musician", "pinner")
      )

      frame.to(ParquetSink(path))

      val actual = ParquetSource(path).toFrame().collect()
      actual shouldBe Vector(
        Row(structType, "clint eastwood", "actor", "carmel"),
        Row(structType, "elton john", "musician", "pinner")
      )
    }
    "read multiple parquet files using file expansion" in {

      val path1 = new Path("test1.pq")
      if (fs.exists(path1))
        fs.delete(path1, false)
      new File(path1.toString).deleteOnExit()

      val path2 = new Path("test2.pq")
      if (fs.exists(path2))
        fs.delete(path2, false)

      val structType = StructType(
        Field("name", StringType, nullable = false),
        Field("location", StringType, nullable = false)
      )

      val frame = Frame.fromValues(
        structType,
        Vector("clint eastwood", "carmel"),
        Vector("elton john", "pinner")
      )

      frame.to(ParquetSink(path1))
      frame.to(ParquetSink(path2))

      val parent = Paths.get(path1.toString).toAbsolutePath.resolve("*")
      val actual = ParquetSource(parent.toString).toFrame().toSet()
      actual shouldBe Set(
        Row(structType, "clint eastwood", "carmel"),
        Row(structType, "elton john", "pinner"),
        Row(structType, "clint eastwood", "carmel"),
        Row(structType, "elton john", "pinner")
      )
    }
    "support arrays of strings" in {

      val structType = StructType(
        Field("system", StringType),
        Field("planets", ArrayType(StringType))
      )

      val sol = Vector("sol", Vector("earth", "mars", "saturn"))
      val algeron = Vector("algeron", Vector("algeron-i", "algeron-ii", "algeron-iii"))
      val frame = Frame.fromValues(structType, sol, algeron)

      val path = new Path("array.pq")
      if (fs.exists(path))
        fs.delete(path, false)

      frame.to(ParquetSink(path))

      val rows = ParquetSource(path).toFrame().collect()
      rows.head.schema shouldBe structType
      rows shouldBe Vector(
        Row(structType, sol),
        Row(structType, algeron)
      )

      fs.delete(path, false)
    }
    "support nested structs" in {

      val structType = StructType(
        Field("name", StringType),
        Field("homeworld", StructType(
          Field("name", StringType),
          Field("x", IntType.Signed),
          Field("y", IntType.Signed),
          Field("z", IntType.Signed)
        ))
      )

      val frame = Frame.fromValues(
        structType,
        Vector("federation", Vector("sol", 0, 0, 0)),
        Vector("empire", Vector("andromeda", 914, 735, 132))
      )

      val path = new Path("struct.pq")
      if (fs.exists(path))
        fs.delete(path, false)

      frame.to(ParquetSink(path))

      val rows = ParquetSource(path).toFrame().collect()
      rows shouldBe Seq(
        Row(structType, Vector("federation", Vector("sol", 0, 0, 0))),
        Row(structType, Vector("empire", Vector("andromeda", 914, 735, 132)))
      )

      fs.delete(path, false)
    }
  }
}
