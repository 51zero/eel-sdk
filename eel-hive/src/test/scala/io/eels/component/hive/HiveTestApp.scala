package io.eels.component.hive

import com.sksamuel.exts.metrics.Timed
import io.eels.Frame
import io.eels.schema.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.util.Random

object HiveTestApp extends App with Timed {

  val Database = "sam"
  val Table = "foo1"

  val conf = new Configuration
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.reloadConfiguration()

  implicit val fs = FileSystem.get(conf)

  implicit val hiveConf = new HiveConf()
  hiveConf.addResource(new Path("/home/sam/development/hive-2.1.0-bin/conf/hive-site.xml"))
  hiveConf.reloadConfiguration()

  implicit val client = new HiveMetaStoreClient(hiveConf)

  val data = Array(
    Vector("elton", "yellow brick road ", "1972"),
    Vector("elton", "tumbleweed connection", "1974"),
    Vector("elton", "empty sky", "1969"),
    Vector("beatles", "white album", "1969"),
    Vector("beatles", "tumbleweed connection", "1966"),
    Vector("pinkfloyd", "the wall", "1979"),
    Vector("pinkfloyd", "dark side of the moon", "1974"),
    Vector("pinkfloyd", "emily", "1966")
  )

  val rows = List.fill(10000)(data(Random.nextInt(data.length)))
  val frame = Frame.fromValues(Schema("artist", "album", "year"), rows).addField("bibble", "myvalue").addField("timestamp", System.currentTimeMillis)
  println(frame.schema.show())

  timed("creating table") {
    new HiveOps(client).createTable(
      Database,
      Table,
      frame.schema,
      List("artist"),
      format = HiveFormat.Parquet,
      overwrite = true
    )
  }

  val table = new HiveOps(client).tablePath(Database, Table)

  val sink = HiveSink(Database, Table).withIOThreads(4)
  timed("writing data") {
    frame.to(sink)
    logger.info("Write complete")
  }

  //  val footers = ParquetSource(s"hdfs:/user/hive/warehouse/$Database.db/$Table/*").footers
  //
  //  import scala.collection.JavaConverters._
  //
  //  val sum = footers.flatMap(_.getParquetMetadata.getBlocks.asScala.map(_.getRowCount)).sum
  //  println(sum)


  //  timed("hive read") {
  //    val source = HiveSource("sam", "albums").toFrame(4).filter("year", _.toString == "1979")
  //    println(source.size)
  //  }
  //
  //  timed("hive read with predicate") {
  //    val source = HiveSource("sam", "albums").withPredicate(PredicateEquals("year", "1979")).toFrame(4)
  //    println(source.size)
  //  }

//  val size = HiveSource(Database, Table).toFrame(4).size()
 // println(size)

 // val k = HiveSource(Database, Table).withProjection("album").toFrame(4).take(3).toList()
 // println(k)

  val m = HiveSource(Database, Table).withProjection("artist").toFrame(4).take(3).toList()
  println(m)

  //val partitionNames = client.listPartitionNames("sam", "albums", Short.MaxValue)
  //  println(partitionNames.asScala.toList)

  //  val result = HiveSource("sam", "albums").withPartitionConstraint("year", "<", "1975").toSeq
  //  logger.info("Result=" + result)

  //  val years = HiveSource("sam", "albums").withPartitionConstraint("year", "<", "1970").withColumns("year", "artist").toSeq
  //  logger.info("years=" + years)
}
