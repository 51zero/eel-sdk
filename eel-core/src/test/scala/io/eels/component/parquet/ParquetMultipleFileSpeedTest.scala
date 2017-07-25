package io.eels.component.parquet

import java.io.File

import com.sksamuel.exts.metrics.Timed
import io.eels.component.parquet.util.ParquetLogMute
import io.eels.datastream.DataStream
import io.eels.schema.StructType
import io.eels.{FilePattern, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Random

/**
  * v1.2.0-M6
  * 5m rows random contents; 20 parts using reactivex flows; reading parquet=2700
  * v1.2.0-M10
  * 5m rows random contents; 20 parts using eels subscriber; reading parquet=1200
  * v1.2.0-M11
  * 5m rows random contents; 20 parts using eels subscriber; reading parquet=1400
  * v1.3.0-snapshot1
  * 5m rows random contents; 20 parts using eels subscriber; writing=15000, reading parquet=1400
  */
object ParquetMultipleFileSpeedTest extends App with Timed {
  ParquetLogMute()

  val size = 5000000
  val count = 20
  val schema = StructType("a", "b", "c", "d", "e")

  def createRow = Row(schema, Random.nextBoolean(), Random.nextFloat(), Random.nextGaussian(), Random.nextLong(), Random.nextString(4))

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(new Configuration())

  val dir = new Path("parquet-speed-test")
  new File(dir.toString).mkdirs()

  new File(dir.toString).listFiles().foreach(_.delete)
  timed("Insertion") {
    val ds = DataStream.fromIterator(schema, Iterator.continually(createRow).take(size))
    ds.to(ParquetSink(new Path("parquet-speed-test/parquet_speed.pq")), count)
  }

  for (_ <- 1 to 25) {
    assert(count == FilePattern("parquet-speed-test/*").toPaths().size)

    timed("Reading with ParquetSource") {
      val actual = ParquetSource("parquet-speed-test/*").toDataStream().map { row => row }.filter(_ => true).size
      assert(actual == size, s"Expected $size but was $actual")
    }

    println("")
    println("---------")
    println("")
  }
}
