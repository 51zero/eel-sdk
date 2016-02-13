package io.eels.component.hive

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.Frame
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

  val frame = Frame(
    Map("name" -> "tyrion", "house" -> "lanister"),
    Map("name" -> "jaime", "house" -> "lanister"),
    Map("name" -> "arya", "house" -> "stark"),
    Map("name" -> "sansa", "house" -> "stark"),
    Map("name" -> "roose bolton", "house" -> "bolton"),
    Map("name" -> "ramsey bolton", "house" -> "bolton")
  )

  fs.mkdirs(new Path("/bigdata/sam/characters"))

  HiveOps.createTable(
    "sam",
    "characters",
    frame.schema,
    List("house"),
    format = HiveFormat.Parquet,
    location = Some("hdfs://localhost:9000/bigdata/sam/characters"),
    overwrite = true
  )

  val sink = HiveSink("sam", "characters").withPartitions("house")
  frame.to(sink).run
  logger.info("Write complete")

  val plan = HiveSource("sam", "characters").withPartition("house", ">=", "lanister").toList
  logger.info("Result=" + plan.run)

}
