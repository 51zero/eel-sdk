package io.eels.component.parquet

import io.eels.schema._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.scalatest.{FlatSpec, Matchers}


class ParquetCompatibilityTest extends FlatSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(new Configuration())

  "ParquetReader" should "support parquet files generated in spark" in {

    val path = new Path(getClass.getResource("/io/eels/component/parquet/spark.parquet").getFile)
    val reader = new ParquetReader[Group](conf, path, new GroupReadSupport)
    val group = reader.read()
    println(group)

    val frame = ParquetSource(path).toFrame()
    println(frame.schema)
    frame.schema shouldBe StructType(
      Field("myDouble", DoubleType),
      Field("myLong", LongType.Signed),
      Field("myInt", IntType.Signed),
      Field("myBoolean", BooleanType),
      Field("myFloat", FloatType),
      Field("myShort", ShortType.Signed),
      Field("myDecimal", DecimalType(Precision(38), Scale(18))),
      Field("myBytes", BinaryType),
      Field("myDate", DateType),
      Field("myTimestamp", TimestampType)
    )

    //    val row = frame.collect().head
    //    println(row)
    //    row.values shouldBe ""
  }
}