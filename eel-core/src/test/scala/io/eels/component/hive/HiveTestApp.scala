package io.eels.component.hive

import com.sksamuel.scalax.Logging
import com.sksamuel.scalax.metrics.Timed
import io.eels.Frame
import io.eels.component.parquet.ParquetSource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.collection.JavaConverters._
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

  val data = Array(
    Seq("elton", "yellow brick road ", "1972"),
    Seq("elton", "tumbleweed connection", "1974"),
    Seq("elton", "empty sky", "1969"),
    Seq("beatles", "white album", "1969"),
    Seq("beatles", "tumbleweed connection", "1966"),
    Seq("pinkfloyd", "the wall", "1979"),
    Seq("pinkfloyd", "dark side of the moon", "1974"),
    Seq("pinkfloyd", "emily", "1966")
  )

  val rows = List.fill(100000)(data(Random.nextInt(data.length)) ++ List(Random.nextBoolean().toString, Random.nextBoolean.toString, Random.nextBoolean.toString))
  val frame = Frame(Seq("artist", "album", "year", "j", "k", "l"), rows).addColumn("bibble", "myvalue").addColumn("timestamp", System.currentTimeMillis)
  println(frame.schema.print)

  timed("creating table") {
    HiveOps.createTable(
      "sam",
      "albums",
      frame.schema,
      List("artist"),
      format = HiveFormat.Parquet,
      overwrite = true
    )
  }

  val table = HiveOps.tablePath("sam", "albums")

  val sink = HiveSink("sam", "albums").withIOThreads(4)
  timed("writing data") {
    frame.to(sink)
    logger.info("Write complete")
  }

  val footers = ParquetSource("hdfs:/user/hive/warehouse/sam.db/albums/*").footers

  val sum = footers.flatMap(_.getParquetMetadata.getBlocks.asScala.map(_.getRowCount)).sum
  println(sum)

//  timed("hive read") {
//    val source = HiveSource("sam", "albums").toFrame(4).filter("year", _.toString == "1979")
//    println(source.size)
//  }
//
//  timed("hive read with predicate") {
//    val source = HiveSource("sam", "albums").withPredicate(PredicateEquals("year", "1979")).toFrame(4)
//    println(source.size)
//  }

  val counts = HiveSource("sam", "albums").toFrame(4).counts
  println(counts)

  //val partitionNames = client.listPartitionNames("sam", "albums", Short.MaxValue)
  //  println(partitionNames.asScala.toList)

  //  val result = HiveSource("sam", "albums").withPartitionConstraint("year", "<", "1975").toSeq
  //  logger.info("Result=" + result)

  //  val years = HiveSource("sam", "albums").withPartitionConstraint("year", "<", "1970").withColumns("year", "artist").toSeq
  //  logger.info("years=" + years)
}
