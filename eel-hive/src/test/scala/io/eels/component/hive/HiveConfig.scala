package io.eels.component.hive

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient


object HiveConfig {

  val basePath = new File("hadoop-resource-files").getAbsolutePath

  implicit val conf = new Configuration
  conf.addResource(new Path(s"$basePath/core-site.xml"))
  conf.addResource(new Path(s"$basePath/hdfs-site.xml"))
  conf.reloadConfiguration()

  implicit val fs = FileSystem.get(conf)

  implicit val hiveConf = new HiveConf()
  hiveConf.addResource(new Path(s"$basePath/hive-site.xml"))
  hiveConf.reloadConfiguration()

  implicit val client = new HiveMetaStoreClient(hiveConf)

  val ops = new HiveOps(client)
}