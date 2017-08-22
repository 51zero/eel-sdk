package io.eels.component.hive

import com.sksamuel.exts.metrics.Timed
import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema.StructType
import org.apache.hadoop.fs.permission.FsPermission

import scala.util.Random

/**
  * Version 1.0:
  *
  * 1m rows, 4267ms, single thread (prior to adding the multi threaded code)
  *
  * 1m rows, 4200ms, single thread
  * 1m rows, 1275ms, 4 threads
  * 10m rows, 10499ms, 4 threads
  *
  * ORC:
  * 1m rows, 1263, 4 threads
  * 8m rows, 4500ms, 4 threads
  * 8m rows, 4167ms, 4 threads
  *
  *
  */
object HiveSpeedTest extends App with Timed {

  import HiveConfig._

  val Database = "sam"
  val Table = "speedtest"

  val schema = StructType("artist", "album", "year")
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

  def createRow = Row(schema, data(Random.nextInt(data.length)))
  val size = 10000000

  while (true) {

    val ds = DataStream.fromRowIterator(schema, Iterator.continually(createRow).take(size))
      .addField("bibble", "myvalue")
      .addField("timestamp", System.currentTimeMillis.toString)
    println(ds.schema.show())

    HiveTable(Database, Table).drop(true)

    ops.createTable(
      Database,
      Table,
      ds.schema,
      List("artist"),
      overwrite = true
    )

    timed("writing data") {
      val sink = HiveSink(Database, Table).withPermission(new FsPermission("700"))
      ds.to(sink)
      logger.info("Write complete")
    }

    timed("reading data") {
      val source = HiveSource(Database, Table)
      source.toDataStream().size
      logger.info("Read complete")
    }

    Thread.sleep(5000)
  }
}