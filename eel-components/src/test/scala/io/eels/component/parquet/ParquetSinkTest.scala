package io.eels.component.parquet

import io.eels.{Frame, Row}
import io.eels.schema.{Field, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{FlatSpec, Matchers}

class ParquetSinkTest extends FlatSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.get(conf)

  "ParquetSink" should "handle nulls" in {

    val schema = StructType(
      Field("name", StringType, nullable = true),
      Field("job", StringType, nullable = true),
      Field("location", StringType, nullable = true)
    )

    val frame = Frame.fromValues(
      schema,
      Vector("clint eastwood", "actor", null),
      Vector("elton john", null, "pinner")
    )

    val path = new Path("test.pq")
    if (fs.exists(path))
      fs.delete(path, false)

    frame.to(ParquetSink(path))

    val rows = ParquetSource(path).toFrame().collect()
    rows shouldBe Seq(
      Row(schema, Vector("clint eastwood", "actor", null)),
      Row(schema, Vector("elton john", null, "pinner"))
    )

    fs.delete(path, false)
  }
}
