package io.eels.component.parquet

import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema.{Field, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{FlatSpec, Matchers}

class ParquetSinkTest extends FlatSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global
  private implicit val conf = new Configuration()
  private implicit val fs = FileSystem.get(conf)

  "ParquetSink" should "handle nulls" in {

    val schema = StructType(
      Field("name", StringType, nullable = true),
      Field("job", StringType, nullable = true),
      Field("location", StringType, nullable = true)
    )

    val ds = DataStream.fromValues(
      schema,
      Seq(
        Vector("clint eastwood", "actor", null),
        Vector("elton john", null, "pinner")
      )
    )

    val path = new Path("test.pq")
    if (fs.exists(path))
      fs.delete(path, false)

    ds.to(ParquetSink(path))

    val rows = ParquetSource(path).toDataStream().collect
    rows shouldBe Seq(
      Row(schema, Vector("clint eastwood", "actor", null)),
      Row(schema, Vector("elton john", null, "pinner"))
    )
    fs.delete(path, false)
  }

  it should "support overwrite" in {

    val path = new Path("overwrite_test.pq")
    fs.delete(path, false)

    val schema = StructType(Field("a", StringType))
    val ds = DataStream.fromRows(
      schema,
      Seq(
        Row(schema, Vector("x")),
        Row(schema, Vector("y"))
      )
    )

    ds.to(ParquetSink(path))
    ds.to(ParquetSink(path).withOverwrite(true))
    fs.delete(path, false)
  }

  it should "support permissions" in {

    val path = new Path("permissions.pq")

    val schema = StructType(Field("a", StringType))
    val ds = DataStream.fromRows(schema,
      Row(schema, Vector("x")),
      Row(schema, Vector("y"))
    )

    ds.to(ParquetSink(path).withOverwrite(true).withPermission(FsPermission.valueOf("-rw-r----x")))
    fs.getFileStatus(path).getPermission.toString shouldBe "rw-r----x"
    fs.delete(path, false)
  }
}
