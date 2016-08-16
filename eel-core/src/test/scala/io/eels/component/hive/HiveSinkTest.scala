package io.eels.component.hive

import io.eels.Frame
import io.eels.schema.Schema
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.scalatest.{Inspectors, Matchers, WordSpec}

class HiveSinkTest extends WordSpec with Matchers with Inspectors {

  val conf = new HiveConf()
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.addResource(new Path("/home/sam/development/hive-1.2.1-bin/conf/hive-site.xml"))
  conf.reloadConfiguration()

  implicit val client = new HiveMetaStoreClient(conf)
  implicit val ops = new HiveOps(client)
  implicit val fs = FileSystem.get(conf)


  "HiveSink" should {
    "not write partition data to the data file" ignore {

      val tableName = "hivesinktest" + System.currentTimeMillis()

      val schema = Schema("a", "b")
      ops.createTable("sam", tableName, schema, format = HiveFormat.Parquet, partitionKeys = List("b"))

      val frame = Frame(schema, List.fill(100)(List("HOLLY", "QUEEG")))
      frame.to(HiveSink("sam", tableName))

      val table = client.getTable("sam", tableName)
      val paths = HiveFilesFn(table, List("b")).map(_._1.getPath)

      // none of these files should contain col b as that's a partition
      forExactly(0, paths) { it =>
        val ins = fs.open(it)
        val str = IOUtils.toString(ins)
        str should include("QUEEG")
      }

      // all files should contain col a as that's not a partition
      forAll(paths) { it =>
        val ins = fs.open(it)
        val str = IOUtils.toString(ins)
        str should include("HOLLY")
      }
    }
  }
}