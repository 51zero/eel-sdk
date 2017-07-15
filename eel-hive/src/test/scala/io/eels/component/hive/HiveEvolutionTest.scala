package io.eels.component.hive

import java.io.File

import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema.{Field, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.scalatest.{FunSuite, Matchers}

class HiveEvolutionTest extends FunSuite with Matchers {

  val dbname = "sam"
  val table = "evolution_test"

  implicit val conf = new Configuration
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.reloadConfiguration()

  implicit val fs = FileSystem.get(conf)

  implicit val hiveConf = new HiveConf()
  hiveConf.addResource(new Path("/home/sam/development/hive-2.1.0-bin/conf/hive-site.xml"))
  hiveConf.reloadConfiguration()

  implicit val client = new HiveMetaStoreClient(hiveConf)

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
