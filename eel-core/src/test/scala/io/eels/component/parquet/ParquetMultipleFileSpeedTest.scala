package io.eels.component.parquet

import java.io.File

import com.sksamuel.exts.metrics.Timed
import io.eels.Row
import io.eels.component.parquet.util.ParquetLogMute
import io.eels.datastream.DataStream
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Random

/**
  * v1.2.0-snapshot
  * 5m rows random contents, 20 parts reading parquet=620; reading avro=1400
  */
object ParquetMultipleFileSpeedTest extends App with Timed {
  ParquetLogMute()

  val size = 250000
  val count = 20
  val schema = StructType("a", "b", "c", "d", "e")

  def createRow = Row(schema, Random.nextBoolean(), Random.nextFloat(), Random.nextGaussian(), Random.nextLong(), Random.nextString(4))
  val ds = DataStream.fromIterator(schema, Iterator.continually(createRow).take(size))

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(new Configuration())

  val path = new Path("parquet-speed/parquet_speed.pq")
  fs.delete(path, false)

  new File(path.toString).deleteOnExit()

  timed("Insertion") {
    ds.to(ParquetSink(path).withOverwrite(true), count)
  }

  for (_ <- 1 to 5) {

    timed("Reading with ParquetSource") {
      val actual = ParquetSource(path).toDataStream().size
      assert(actual == size)
    }

    println("")
    println("---------")
    println("")

    Thread.sleep(2000)
  }
}
