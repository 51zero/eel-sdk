package io.eels.component.parquet

import com.sksamuel.exts.metrics.Timed
import io.eels.schema.StructType
import io.eels.{Frame, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Random

/**
  * v0.90 1m rows insertion: 2500 reading: 4438
  */
object ParquetSpeedTest extends App with Timed {

  val schema = StructType("a", "b", "c", "d", "e")
  val rows = List.fill(1000000)(Row(schema, Random.nextBoolean(), Random.nextFloat(), Random.nextGaussian(), Random.nextLong(), Random.nextString(10)))
  val frame = Frame(schema, rows)

  implicit val fs = FileSystem.getLocal(new Configuration())

  while (true) {

    val path = new Path("parquet_speed.csv")
    fs.delete(path, false)

    timed("Insertion") {
      frame.to(ParquetSink(path))
    }

    timed("Reading") {
      val in = ParquetSource(path).toFrame(1).toList()
      assert(in.size == rows.size, in.size)
    }

    fs.delete(path, false)
  }
}
