package io.eels.component.hive

import java.io.File

import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema.{Field, StringType, StructType}
import org.scalatest.{FunSuite, Matchers}

class HiveAlignmentTest extends FunSuite with Matchers {

  import HiveConfig._

  private val dbname = "default"
  private val table = "align_test_" + System.currentTimeMillis()

  test("pad a row with nulls") {
    assume(new File(s"$basePath/core-site.xml").exists)

    val schema = StructType(Field("a", StringType), Field("b", StringType, true))

    HiveTable(dbname, table).drop()
    HiveTable(dbname, table).create(schema)

    DataStream.fromValues(schema.removeField("b"), Seq(Seq("a"))).to(HiveSink(dbname, table))

    HiveSource(dbname, table).toDataStream().collect shouldBe Vector(
      Row(schema, Vector("a", null))
    )
  }

  test("align a row with the hive metastore") {
    assume(new File(s"$basePath/core-site.xml").exists)

    HiveTable(dbname, table).drop()

    // correct schema
    val schema1 = StructType(Field("a", StringType), Field("b", StringType, true))
    DataStream.fromValues(schema1, Seq(Seq("a", "b"))).to(HiveSink(dbname, table).withCreateTable(true))

    // reversed schema, the row should be aligned
    val schema2 = StructType(Field("b", StringType), Field("a", StringType, true))
    DataStream.fromValues(schema2, Seq(Seq("b", "a"))).to(HiveSink(dbname, table))

    HiveSource(dbname, table).toDataStream().collect shouldBe Vector(
      Row(schema1, Vector("a", "b")),
      Row(schema1, Vector("a", "b"))
    )
  }
}
