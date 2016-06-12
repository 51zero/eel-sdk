package io.eels.component.hive

import io.eels.schema.Field
import io.eels.schema.Schema
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

fun main(args: Array<String>): Unit {

  val conf = HiveConf()
  conf.addResource(Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.addResource(Path("/home/sam/development/hive-1.2.1-bin/conf/hive-site.xml"))
  conf.reloadConfiguration()

  val client = HiveMetaStoreClient(conf)
  val ops = HiveOps(client)

  ops.createDatabase("sam")

  ops.createTable(
      "sam",
      "test1",
      Schema(Field("name"), Field("artist"), Field("year"), Field("genre"), Field.createStruct("band_members", Field("name"))),
      listOf("genre", "artist"),
      format = HiveFormat.Parquet,
      overwrite = true,
      location = "file:/user/hive/warehouse/sam.db/bibble"
  )

  val provider = HiveProvider("sam", "test1", client)
  val schema = provider.schema()

  println(schema.show())

  println(ops.partitionFieldSchemas("sam", "test1"))
  println(ops.partitions("sam", "test1"))

}