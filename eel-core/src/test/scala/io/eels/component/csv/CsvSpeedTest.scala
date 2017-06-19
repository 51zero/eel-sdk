package io.eels.component.csv

import java.nio.file.Paths

import com.sksamuel.exts.metrics.Timed
import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import scala.util.Random

/**
  * v0.90 1m rows insertion: 1400 reading: 1324
  * v1.10 1m rows insertion: 1250: reading: 680
  */
object CsvSpeedTest extends App with Timed {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(conf)

  val schema = StructType("a", "b", "c", "d", "e")
  val rows = List.fill(1000000)(Row(schema, Random.nextBoolean(), Random.nextFloat(), Random.nextGaussian(), Random.nextLong(), Random.nextString(10)))
  val frame = DataStream.fromRows(schema, rows)

  while(true) {

    val path = Paths.get("csv_speed.csv")
    path.toFile.delete()

    timed("Insertion") {
      frame.to(CsvSink(path))
    }

    timed("Reading") {
      val in = CsvSource(path).toDataStream().collect
      assert(in.size == rows.size, in.size)
    }

    path.toFile.delete()
  }
}
