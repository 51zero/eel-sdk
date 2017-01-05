package io.eels.component.parquet

import java.io.File

import io.eels.Frame
import io.eels.schema.{Field, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{FlatSpec, Matchers}

class ParquetPredicateTest extends FlatSpec with Matchers {

  val schema = StructType(
    Field("name", StringType, nullable = false),
    Field("job", StringType, nullable = false),
    Field("location", StringType, nullable = false)
  )
  val frame = Frame.fromValues(
    schema,
    Vector("clint eastwood", "actor", "carmel"),
    Vector("elton john", "musician", "pinner")
  )

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.get(new Configuration())
  val path = new Path("test.pq")

  if (fs.exists(path))
    fs.delete(path, false)

  new File(path.toString).deleteOnExit()

  frame.to(ParquetSink(path))

  "ParquetSource" should "support predicates" in {
    val rows = ParquetSource(path).withPredicate(Predicate.equals("job", "actor")).toFrame().collect()
    rows.size shouldBe 1
  }
}
