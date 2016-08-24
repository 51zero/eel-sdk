package io.eels.component.hive

import com.sksamuel.exts.metrics.Timed
import io.eels.Frame
import io.eels.schema.Schema
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.util.Random

/**
  * 1m rows, 4267ms, single thread (prior to multi thread code)
  * 1m rows, 4200ms, single thread
  * 1m rows, 1275ms, 4 threads
  * 10m rows, 10499ms, 4 threads
  *
  * ORC:
  * 1m rows, 1263, 4 threads
  * 8m rows, 4500ms, 4 threads
  * 8m rows, 4167ms, 4 threads
  */
object HiveSpeedTest extends App with Timed {

  val conf = new HiveConf()
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.addResource(new Path("/home/sam/development/hive-1.2.1-bin/conf/hive-site.xml"))
  conf.reloadConfiguration()

  implicit val client = new HiveMetaStoreClient(conf)
  implicit val ops = new HiveOps(client)
  implicit val fs = FileSystem.get(conf)

  val Database = "sam"
  val Table = "speedtest"

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

  val rows = List.fill(1000000)(data(Random.nextInt(data.length)))
  val frame = Frame.fromValues(Schema("artist", "album", "year"), rows).addField("bibble", "myvalue").addField("timestamp", System.currentTimeMillis)
  println(frame.schema.show())

  while (true) {

    new HiveOps(client).createTable(
      Database,
      Table,
      frame.schema,
      List("artist"),
      format = HiveFormat.Orc,
      overwrite = true
    )

    val sink = HiveSink(Database, Table).withIOThreads(4)
    timed("writing data") {
      frame.to(sink)
      logger.info("Write complete")
    }

    Thread.sleep(5000)
  }
}