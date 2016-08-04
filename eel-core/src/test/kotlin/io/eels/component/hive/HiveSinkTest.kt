package io.eels.component.hive

import io.eels.Frame
import io.eels.schema.Schema
import io.kotlintest.matchers.have
import io.kotlintest.specs.WordSpec
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

class HiveSinkTest : WordSpec() {

  val conf = HiveConf().apply {
    addResource(Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
    addResource(Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
    addResource(Path("/home/sam/development/hive-1.2.1-bin/conf/hive-site.xml"))
    reloadConfiguration()
  }

  val client = HiveMetaStoreClient(conf)
  val ops = HiveOps(client)
  val fs = FileSystem.get(conf)

  init {

    val tableName = "hivesinktest" + System.currentTimeMillis()

    val schema = Schema("a", "b")
    ops.createTable("sam", tableName, schema, format = HiveFormat.Parquet, partitionKeys = listOf("b"))

    val frame = Frame.create(schema, Array(100) { listOf("HOLLY", "QUEEG") }.toList())
    frame.to(HiveSink("sam", tableName, fs, client))

    "HiveSink" should {
      "not write partition data to the data file" {
        val table = client.getTable("sam", tableName)
        val paths = HiveFilesFn(table, fs, client, listOf("b")).map { it.first.path }
        // none of these files should contain col b as that's a partition
        forNone(paths) {
          val ins = fs.open(it)
          val str = IOUtils.toString(ins)
          str should have substring ("QUEEG")
        }
        // all files should contain col a as that's not a partition
        forAll(paths) {
          val ins = fs.open(it)
          val str = IOUtils.toString(ins)
          str should have substring ("HOLLY")
        }
      }.config(tag = "hive", ignored = true)
    }
  }
}