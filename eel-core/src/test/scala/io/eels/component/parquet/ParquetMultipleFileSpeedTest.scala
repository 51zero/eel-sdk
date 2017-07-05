package io.eels.component.parquet

import java.io.File

import com.sksamuel.exts.metrics.Timed
import io.eels.{FilePattern, Listener, Row}
import io.eels.component.parquet.util.ParquetLogMute
import io.eels.datastream.DataStream
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Random

/**
  * v1.2.0-snapshot
  * 5m rows random contents; 20 parts using reactivex flows; reading parquet=2700
  */
object ParquetMultipleFileSpeedTest extends App with Timed {
  ParquetLogMute()

  val size = 5000000
  val count = 20
  val schema = StructType("a", "b", "c", "d", "e")

  def createRow = Row(schema, Random.nextBoolean(), Random.nextFloat(), Random.nextGaussian(), Random.nextLong(), Random.nextString(4))
  val ds = DataStream.fromIterator(schema, Iterator.continually(createRow).take(size))

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(new Configuration())

  val dir = new Path("parquet-speed")
  new File(dir.toString).mkdirs()
  new File(dir.toString).listFiles().foreach(_.delete)

  timed("Insertion") {
    ds.to(ParquetSink(new Path("parquet-speed/parquet_speed.pq")), count)
  }

  for (_ <- 1 to 5) {
    assert(count == FilePattern("parquet-speed/*").toPaths().size)

    timed("Reading with ParquetSource ds1") {
      val actual = ParquetSource("parquet-speed/*").toDataStream().size
      assert(actual == size, s"Expected $size but was $actual")
    }

    println("")
    println("---------")
    println("")

    //    timed("Reading with ParquetSource ds2") {
    //      val actual = ParquetSource("parquet-speed/*").toDataStream2.listener(new Listener {
    //        override def onNext(row: Row): Unit = ()
    //      }).size
    //      assert(actual == size, s"Expected $size but was $actual")
    //    }
    //
    //    println("")
    //    println("---------")
    //    println("")

  }
}
