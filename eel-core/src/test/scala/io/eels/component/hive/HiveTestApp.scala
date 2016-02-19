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

  val s = client.getSchema("sam", "schematest2")
  println(s)

  val frame = Frame(
    Map("artist" -> "elton", "album" -> "yellow brick road", "year" -> "1972"),
    Map("artist" -> "elton", "album" -> "tumbleweed connection", "year" -> "1974"),
    Map("artist" -> "elton", "album" -> "empty sky", "year" -> "1969"),
    Map("artist" -> "beatles", "album" -> "white album", "year" -> "1696"),
    Map("artist" -> "beatles", "album" -> "tumbleweed connection", "year" -> "1966"),
    Map("artist" -> "pinkfloyd", "album" -> "the wall", "year" -> "1979"),
    Map("artist" -> "pinkfloyd", "album" -> "dark side of the moon", "year" -> "1974"),
    Map("artist" -> "pinkfloyd", "album" -> "emily", "year" -> "1966")
  )

  HiveOps.createTable(
    "sam",
    "albums",
    frame.schema,
    List("year", "artist"),
    format = HiveFormat.Parquet,
    overwrite = true
  )

  val sink = HiveSink("sam", "albums").withIOThreads(2)
  frame.to(sink).run
  logger.info("Write complete")

  val plan = HiveSource("sam", "albums").withPartition("year", "<", "1975").toSeq
  logger.info("Result=" + plan.run)

  val parts = client.listPartitions("sam", "albums", Short.MaxValue)
  println(parts)
  val q = parts
}
