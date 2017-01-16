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

      fs.delete(path1, false)
      fs.delete(path2, false)
    }
    "support collections of strings" in {

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
      rows.map(_.values).head(1).asInstanceOf[Seq[String]] shouldBe Vector("earth", "mars", "saturn")
      rows.map(_.values).last(1).asInstanceOf[Seq[String]] shouldBe Vector("algeron-i", "algeron-ii", "algeron-iii")

      fs.delete(path, false)
    }
    "support collections of doubles" in {

      val structType = StructType(
        Field("name", StringType),
        Field("doubles", ArrayType(DoubleType))
      )

      val values1 = Vector("a", Vector(0.1, 0.2, 0.3))
      val values2 = Vector("b", Vector(0.3, 0.4, 0.5))
      val frame = Frame.fromValues(structType, values1, values2)

      val path = new Path("array.pq")
      if (fs.exists(path))
        fs.delete(path, false)

      frame.to(ParquetSink(path))

      val rows = ParquetSource(path).toFrame().collect()
      rows.head.schema shouldBe structType
      rows.map(_.values).head(1).asInstanceOf[Seq[Double]].toVector shouldBe Vector(0.1, 0.2, 0.3)
      rows.map(_.values).last(1).asInstanceOf[Seq[Double]].toVector shouldBe Vector(0.3, 0.4, 0.5)

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
    "support maps" in {

      val structType = StructType(
        Field("name", StringType),
        Field("map", MapType(StringType, BooleanType))
      )

      val frame = Frame.fromValues(structType, Vector("abc", Map("a" -> true, "b" -> false)))

      val path = new Path("maps.pq")
      if (fs.exists(path))
        fs.delete(path, false)

      frame.to(ParquetSink(path))

      val rows = ParquetSource(path).toFrame().collect()
      structType shouldBe rows.head.schema
      rows shouldBe Seq(Row(structType, Vector("abc", Map("a" -> true, "b" -> false))))

      fs.delete(path, false)
    }
  }
}
