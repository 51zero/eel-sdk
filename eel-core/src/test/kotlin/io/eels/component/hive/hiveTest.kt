package io.eels.component.hive

import org.apache.hadoop.fs.FileSystem
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
  val fs = FileSystem.getLocal(conf)

  val dbName = "sam"
  val tableName = "people3"

//  ops.createDatabase("sam")
//
//  ops.createTable(
//      "sam",
//      "test1",
//      Schema(Field("name"), Field("artist"), Field("year"), Field("genre"), Field.createStruct("band_members", Field("name"))),
//      listOf("genre", "artist"),
//      format = HiveFormat.Parquet,
//      overwrite = false,
//      location = "file:/user/hive/warehouse/sam.db/bibble"
//  )

  val database = HiveDatabase(dbName, fs, client)
  val tables = database.tables()
  println(tables)

  val table = database.table(tableName)
  val schema = table.schema()

  println(schema.show())

  val partitions = ops.partitions(dbName, tableName)
  val partitionKeys = client.getTable(dbName, tableName).partitionKeys
  val partitionNames = client.listPartitionNames(dbName, tableName, Short.MAX_VALUE)

  val eelPartitionKeys = table.partitionKeys()
  val eelPartitions = table.partitions()
  println(eelPartitions)
  val eelPartitionNames = table.partitionNames()

}