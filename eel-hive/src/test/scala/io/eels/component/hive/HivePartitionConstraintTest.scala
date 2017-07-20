package io.eels.component.hive

import java.io.File

import io.eels.datastream.DataStream
import io.eels.schema.{Field, PartitionConstraint, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.scalatest.{FunSuite, Matchers}

class HivePartitionConstraintTest extends FunSuite with Matchers {

  val dbname = "sam"
  val table = "constraint_test"

  implicit val conf = new Configuration
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.reloadConfiguration()

  implicit val fs = FileSystem.get(conf)

  implicit val hiveConf = new HiveConf()
  hiveConf.addResource(new Path("/home/sam/development/hive-2.1.0-bin/conf/hive-site.xml"))
  hiveConf.reloadConfiguration()

  implicit val client = new HiveMetaStoreClient(hiveConf)

  HiveTable(dbname, table).drop()
  val schema = StructType(
    Field("state", StringType),
    Field("capital", StringType)
  )

  test("non existing partitions in constraint should return no data") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)

    DataStream.fromValues(schema, Seq(
      Seq("iowa", "des moines"),
      Seq("maine", "augusta")
    )).to(HiveSink(dbname, table).withCreateTable(true, Seq("state")))

    HiveSource(dbname, table)
      .withPartitionConstraint(PartitionConstraint.equals("state", "pa"))
      .toDataStream()
      .collect.size shouldBe 0
  }
}
