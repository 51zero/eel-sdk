package io.eels.component.hive

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

object HiveTestApp extends App with StrictLogging {

  val conf = new Configuration
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.reloadConfiguration()

  implicit val fs = FileSystem.get(conf)

  implicit val hiveConf = new HiveConf()
  hiveConf.addResource(new Path("/home/sam/development/hive-1.2.1-bin/conf/hive-site.xml"))
  hiveConf.reloadConfiguration()

  implicit val client = new HiveMetaStoreClient(hiveConf)
  HiveOps.createTable("sam", "songs", true)

  logger.info("Result=" + HiveSource("sam", "songs").toList.run)
}
