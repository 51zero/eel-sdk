package io.eels.component.orc

import java.io.File

import com.sksamuel.exts.metrics.Timed
import io.eels.datastream.DataStream
import io.eels.schema.StructType
import io.eels.{FilePattern, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Random

/**
  * v1.3.0
  * 5m rows random contents; 20 parts; reading=1500
  */
object OrcMultipleFileSpeedTest extends App with Timed {

  val size = 5000000
  val count = 20
  val schema = StructType("a", "b", "c", "d", "e")

  def createRow = Row(schema, Random.nextBoolean(), Random.nextFloat(), Random.nextGaussian(), Random.nextLong(), Random.nextString(4))

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(new Configuration())

  val dir = new Path("orc-speed-test")
  new File(dir.toString).mkdirs()

  timed("Insertion") {
    val ds = DataStream.fromIterator(schema, Iterator.continually(createRow).take(size))
    new File(dir.toString).listFiles().foreach(_.delete)
    ds.to(OrcSink(new Path("orc-speed-test/orc_speed.pq")).withOverwrite(true), count)
  }

  for (_ <- 1 to 25) {
    assert(count == FilePattern("orc-speed-test/*").toPaths().size)

    timed("Reading with OrcSource") {
      val actual = OrcSource("orc-speed-test/*").toDataStream().map { row => row }.filter(_ => true).size
      assert(actual == size, s"Expected $size but was $actual")
    }

    println("")
    println("---------")
    println("")
  }
}
