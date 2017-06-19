package io.eels.component.orc

import java.io.File

import io.eels.Predicate
import io.eels.datastream.DataStream
import io.eels.schema.{Field, LongType, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class OrcPredicateTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val schema = StructType(
    Field("name", StringType, nullable = true),
    Field("city", StringType, nullable = true),
    Field("age", LongType.Signed, nullable = true)
  )

  val values = Vector.fill(1000) {
    Vector("sam", "middlesbrough", 37)
  } ++ Vector.fill(1000) {
    Vector("laura", "iowa city", 24)
  }

  val ds = DataStream.fromValues(schema, values)

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.get(new Configuration())
  val path = new Path("test.orc")

  if (fs.exists(path))
    fs.delete(path, false)

  new File(path.toString).deleteOnExit()

  ds.to(OrcSink(path).withRowIndexStride(1000))

  override protected def afterAll(): Unit = fs.delete(path, false)

  "OrcSource" should "support string equals predicates" in {
    conf.set("eel.orc.predicate.row.filter", "false")
    val rows = OrcSource(path).withPredicate(Predicate.equals("name", "sam")).toDataStream().collect
    rows.map(_.values).toSet shouldBe Set(Vector("sam", "middlesbrough", 37L))
  }

  it should "support gt predicates" in {
    conf.set("eel.orc.predicate.row.filter", "false")
    val rows = OrcSource(path).withPredicate(Predicate.gt("age", 30L)).toDataStream().collect
    rows.map(_.values).toSet shouldBe Set(Vector("sam", "middlesbrough", 37L))
  }

  it should "support lt predicates" in {
    conf.set("eel.orc.predicate.row.filter", "false")
    val rows = OrcSource(path).withPredicate(Predicate.lt("age", 30)).toDataStream().collect
    rows.map(_.values).toSet shouldBe Set(Vector("laura", "iowa city", 24L))
  }

  it should "enable row level filtering with predicates by default" in {
    conf.set("eel.orc.predicate.row.filter", "true")
    val rows = OrcSource(path).withPredicate(Predicate.equals("name", "sam")).toDataStream().collect
    rows.head.schema shouldBe schema
    rows.head.values shouldBe Vector("sam", "middlesbrough", 37L)
  }
}
