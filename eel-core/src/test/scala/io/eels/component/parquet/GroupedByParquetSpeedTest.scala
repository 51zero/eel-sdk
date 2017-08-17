package io.eels.component.parquet

import java.util.concurrent.Executors

import com.sksamuel.exts.metrics.Timed
import io.eels.component.parquet.util.ParquetLogMute
import io.eels.schema._
import io.eels.{Listener, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import scala.io.Source

object GroupedByParquetSpeedTest extends App with Timed {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(new Configuration)
  ParquetLogMute()

  val text = getClass.getResourceAsStream("/huckleberry_finn.txt")
  val string = Source.fromInputStream(text).mkString

  val schema = StructType(
    Field("letter", StringType),
    Field("word", StringType)
  )

//  fs.delete(new Path("./parquettest"), true)
//  for (k <- 1 to 300) {
//    val rows = string.split(' ').distinct.map { word =>
//      Row(schema, Vector(word.substring(0), word))
//    }.toSeq
//
//    val path = new Path(s"./parquettest/huck$k.parquet")
//    fs.delete(path, false)
//    ParquetSink(path).write(rows)
//    println(s"Written $path")
//  }

  val executor = Executors.newFixedThreadPool(4)

  while (true) {
    timed("multiple files") {

      val f = ParquetSource("./parquettest/*")
        .toDataStream(new Listener {
          var count = 0
          override def onNext(row: Row): Unit = {
            count = count + 1
            if (count % 250000 == 0)
              println(count)
          }
        })
      println(f.collect.last)
    }
  }

  executor.shutdown()
}
