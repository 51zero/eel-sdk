package io.eels.component.parquet

import java.io.File
import java.nio.file.Paths

import io.eels.Row
import io.eels.component.parquet.avro.{AvroParquetSink, AvroParquetSource}
import io.eels.datastream.DataStream
import io.eels.schema._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class AvroParquetComponentTest extends WordSpec with Matchers {

  private implicit val conf = new Configuration()
  private implicit val fs = FileSystem.get(new Configuration())

  "AvroParquet" should {
    "write and read parquet files" in {

      val path = new Path("test.pq")
      if (fs.exists(path))
        fs.delete(path, false)

      val structType = StructType(
        Field("name", StringType, nullable = false),
        Field("job", StringType, nullable = false),
        Field("location", StringType, nullable = false)
      )

      val ds = DataStream.fromValues(
        structType,
        Seq(
          Array("clint eastwood", "actor", "carmel"),
          Array("elton john", "musician", "pinner")
        )
      )

      ds.to(AvroParquetSink(path))

      val actual = AvroParquetSource(path).toDataStream().collect
      actual shouldBe Vector(
        Array("clint eastwood", "actor", "carmel"),
        Array("elton john", "musician", "pinner")
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

      val frame = DataStream.fromValues(
        structType,
        Seq(
          Array("clint eastwood", "carmel"),
          Array("elton john", "pinner")
        )
      )

      frame.to(AvroParquetSink(path1))
      frame.to(AvroParquetSink(path2))

      val parent = Paths.get(path1.toString).toAbsolutePath.resolve("*")
      val actual = AvroParquetSource(parent.toString).toDataStream().toSet
      actual shouldBe Set(
        Array("clint eastwood", "carmel"),
        Array("elton john", "pinner"),
        Array("clint eastwood", "carmel"),
        Array("elton john", "pinner")
      )
    }
    "support collections of strings" in {

      val structType = StructType(
        Field("system", StringType),
        Field("planets", ArrayType(StringType))
      )

      val sol = Array[Any]("sol", Vector("earth", "mars", "saturn"))
      val algeron = Array[Any]("algeron", Vector("algeron-i", "algeron-ii", "algeron-iii"))
      val frame = DataStream.fromValues(structType, Seq(sol, algeron))

      val path = new Path("array.pq")
      if (fs.exists(path))
        fs.delete(path, false)

      frame.to(AvroParquetSink(path))

      val rows = AvroParquetSource(path).toDataStream().collect
      rows.head.asInstanceOf[Array[String]].toVector shouldBe sol.last
      rows.last.asInstanceOf[Array[String]].toVector shouldBe algeron.last

      fs.delete(path, false)
    }
    "support collections of doubles" in {

      val structType = StructType(
        Field("name", StringType),
        Field("doubles", ArrayType(DoubleType))
      )

      val values1 = Array[Any]("a", Vector(0.1, 0.2, 0.3))
      val values2 = Array[Any]("b", Vector(0.3, 0.4, 0.5))
      val frame = DataStream.fromValues(structType, Seq(values1, values2))

      val path = new Path("array.pq")
      if (fs.exists(path))
        fs.delete(path, false)

      frame.to(AvroParquetSink(path))

      val rows = AvroParquetSource(path).toDataStream().collect
      rows shouldBe Vector(values1, values2)
      fs.delete(path, false)
    }
    "support nested structs" in {

      val structType = StructType(
        Field("name", StringType),
        Field("homeworld", StructType(
          Field("name", StringType, false),
          Field("x", IntType.Signed, false),
          Field("y", IntType.Signed, false),
          Field("z", IntType.Signed, false)
        ))
      )

      val ds = DataStream.fromValues(
        structType,
        Seq(
          Array("federation", Vector("sol", 0, 0, 0)),
          Array("empire", Vector("andromeda", 914, 735, 132))
        )
      )

      val path = new Path("struct.pq")
      if (fs.exists(path))
        fs.delete(path, false)

      ds.to(AvroParquetSink(path))

      val rows = AvroParquetSource(path).toDataStream().collect
      rows shouldBe Vector(
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

      val ds = DataStream.fromValues(structType, Seq(Array("abc", Map("a" -> true, "b" -> false))))

      val path = new Path("maps.pq")
      if (fs.exists(path))
        fs.delete(path, false)

      ds.to(AvroParquetSink(path))

      val rows = AvroParquetSource(path).toDataStream().collect
      rows shouldBe Seq(Row(structType, Vector("abc", Map("a" -> true, "b" -> false))))

      fs.delete(path, false)
    }
  }
}
