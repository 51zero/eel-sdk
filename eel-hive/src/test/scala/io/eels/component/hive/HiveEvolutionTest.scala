package io.eels.component.hive

import java.io.File
import java.nio.file.Paths

import com.sksamuel.exts.io.RecursiveDelete
import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema.{Field, StringType, StructType}
import org.scalatest.{FunSuite, Matchers}

class HiveEvolutionTest extends FunSuite with Matchers with HiveConfig {

  val dbname = "sam"
  val table = "evolution_test_" + System.currentTimeMillis()
  RecursiveDelete(Paths.get("metastore_db"))

  test("allow columns to be added to a hive table") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)
    HiveTable (dbname, table).drop()

    val schema1 = StructType(Field("a", StringType))
    DataStream.fromValues(schema1, Seq(Seq("a"))).to(HiveSink(dbname, table).withCreateTable(true))

    val schema2 = StructType(Field("a", StringType), Field("b", StringType))
    DataStream.fromValues(schema2, Seq(Seq("a", "b"))).to(HiveSink(dbname, table))

    HiveSource(dbname, table).schema shouldBe schema2
  }

  test("pad a row with nulls") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)

    val schema = StructType(Field("a", StringType), Field("b", StringType, true))

    HiveTable(dbname, table).drop()
    HiveTable(dbname, table).create(schema)

    DataStream.fromValues(schema.removeField("b"), Seq(Seq("a"))).to(HiveSink(dbname, table))

    HiveSource(dbname, table).toDataStream().collect shouldBe Vector(
      Row(schema, Vector("a", null))
    )
  }

  test("align a row with the hive metastore") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)

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
