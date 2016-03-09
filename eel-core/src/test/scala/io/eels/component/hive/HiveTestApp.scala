package io.eels.component.hive

import com.sksamuel.scalax.Logging
import com.sksamuel.scalax.metrics.Timed
import io.eels.Frame
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.util.Random

object HiveTestApp extends App with Logging with Timed {

  import scala.concurrent.ExecutionContext.Implicits.global

  val conf = new Configuration
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.reloadConfiguration()

  implicit val fs = FileSystem.get(conf)

  implicit val hiveConf = new HiveConf()
  hiveConf.addResource(new Path("/home/sam/development/hive-1.2.1-bin/conf/hive-site.xml"))
  hiveConf.reloadConfiguration()

  implicit val client = new HiveMetaStoreClient(hiveConf)

  val maps = Array(
    Map("artist" -> "elton", "album" -> "yellow brick road", "year" -> "1972"),
    Map("artist" -> "elton", "album" -> "tumbleweed connection", "year" -> "1974"),
    Map("artist" -> "elton", "album" -> "empty sky", "year" -> "1969"),
    Map("artist" -> "beatles", "album" -> "white album", "year" -> "1969"),
    Map("artist" -> "beatles", "album" -> "tumbleweed connection", "year" -> "1966"),
    Map("artist" -> "pinkfloyd", "album" -> "the wall", "year" -> "1979"),
    Map("artist" -> "pinkfloyd", "album" -> "dark side of the moon", "year" -> "1974"),
    Map("artist" -> "pinkfloyd", "album" -> "emily", "year" -> "1966")
  )

  val rows = List.fill(300)(maps(Random.nextInt(maps.length)))
  val frame = Frame(rows).addColumn("bibble", "myvalue").addColumn("timestamp", System.currentTimeMillis)

  timed("creating table") {
    HiveOps.createTable(
      "sam",
      "albums",
      frame.schema,
      List("year", "artist"),
      format = HiveFormat.Parquet,
      overwrite = true
    )
  }

  val sink = HiveSink("sam", "albums").withIOThreads(4)
  timed("writing data") {
    frame.to(sink)
    logger.info("Write complete")
  }

  val source = HiveSource("sam", "albums").withColumns("year")
  println(source.toSeq)


  //  val result = HiveSource("sam", "albums").withPartitionConstraint("year", "<", "1975").toSeq
  //  logger.info("Result=" + result)

  //  val years = HiveSource("sam", "albums").withPartitionConstraint("year", "<", "1970").withColumns("year", "artist").toSeq
  //  logger.info("years=" + years)
}
