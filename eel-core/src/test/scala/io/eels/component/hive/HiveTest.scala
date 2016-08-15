package io.eels.component.hive

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

object HiveTest extends App {

  val conf = new HiveConf()
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.addResource(new Path("/home/sam/development/hive-1.2.1-bin/conf/hive-site.xml"))
  conf.reloadConfiguration()

  implicit val client = new HiveMetaStoreClient(conf)
  implicit val ops = new HiveOps(client)
  implicit val fs = FileSystem.get(conf)

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

  val database = HiveDatabase(dbName)
  val tables = database.tables()
  println(tables)

  val table = database.table(tableName)
  val schema = table.schema()

  println(schema.show())

  val partitions = ops.partitions(dbName, tableName)
  val partitionKeys = client.getTable(dbName, tableName).getPartitionKeys
  val partitionNames = client.listPartitionNames(dbName, tableName, Short.MaxValue)

  val eelPartitionKeys = table.partitionKeys()
  val eelPartitions = table.partitions()
  println(eelPartitions)
  val eelPartitionNames = table.partitionNames()

  val source = table.toSource()
  val parts = source.parts()
  println(parts)

  val frame = source.toFrame(1)
  val rows = frame.toList()
  println(rows)

}