package io.eels.component.parquet

import io.eels.component.parquet.avro.{AvroParquetSink, AvroParquetSource}
import io.eels.datastream.DataStream
import io.eels.schema.{Field, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{FlatSpec, Matchers}

// tests that avro source/sink and avro parquet source/sink can write/read each others files
class AvroAndParquetCrossCompatibilityTest extends FlatSpec with Matchers {

  private implicit val conf = new Configuration()
  private implicit val fs = FileSystem.get(new Configuration())

  "AvroParquetSource and ParquetSource" should "be compatible" in {

    val path = new Path("cross.pq")
    if (fs.exists(path))
      fs.delete(path, false)

    val structType = StructType(
      Field("name", StringType, nullable = false),
      Field("location", StringType, nullable = false)
    )

    val ds = DataStream.fromValues(
      structType,
      Seq(
        Vector("clint eastwood", "carmel"),
        Vector("elton john", "pinner")
      )
    )

    ds.to(ParquetSink(path))
    AvroParquetSource(path).toDataStream().collect shouldBe ds.collect
    fs.delete(path, false)

    ds.to(AvroParquetSink(path))
    ParquetSource(path).toDataStream().collect shouldBe ds.collect
    fs.delete(path, false)
  }
}
