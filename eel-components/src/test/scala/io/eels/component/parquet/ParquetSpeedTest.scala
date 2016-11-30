package io.eels.component.parquet

import com.sksamuel.exts.metrics.Timed
import io.eels.schema.StructType
import io.eels.{Frame, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Random

/**
  * v0.90 1m rows insertion: 2500 reading: 4438
  * v1.1 1m rows insertion 3400 reading: 2800
  * 1m rows string of length 4; writing=3397;reading=1186
  * 2m rows string of length 4; writing=7725;reading=5479
  * 2m rows string of length 4; writing=7766;reading=5177 dictionary enabled for read
  */
object ParquetSpeedTest extends App with Timed {
  ParquetLogMute()

  val schema = StructType("a", "b", "c", "d", "e")
  val rows = List.fill(2000000)(Row(schema, Random.nextBoolean(), Random.nextFloat(), Random.nextGaussian(), Random.nextLong(), Random.nextString(4)))
  val frame = Frame(schema, rows)

  implicit val fs = FileSystem.getLocal(new Configuration())

  while (true) {

    val path = new Path("parquet_speed.csv")
    fs.delete(path, false)

    timed("Insertion") {
      frame.to(ParquetSink(path))
    }

    timed("Reading") {
      val in = ParquetSource(path).toFrame().collect()
      assert(in.size == rows.size, in.size)
    }

    fs.delete(path, false)
  }
}
