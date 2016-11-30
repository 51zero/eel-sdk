package io.eels.component.parquet

import java.util
import java.util.concurrent.Executors

import com.sksamuel.exts.metrics.Timed
import io.eels.schema._
import io.eels.{Listener, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Random

object ParquetReadTest extends App with Timed {

  implicit val fs = FileSystem.getLocal(new Configuration)
  ParquetLogMute()

  val text = getClass.getResourceAsStream("/huckleberry_finn.txt")
  val string = Source.fromInputStream(text).mkString

  val schema = StructType(
    Field("word", StringType),
    Field("doubles", ArrayType(DoubleType))
  )

  if (!fs.exists(new Path("./parquettest"))) {
    for (k <- 1 to 50) {
      val rows = string.split(' ').distinct.map { word =>
        Row(schema, Vector(word, List.fill(2)(Random.nextDouble)))
      }.toSeq

      val path = new Path(s"./parquettest/huck$k.parquet")
      fs.delete(path, false)
      ParquetSink(path).write(rows)
      println(s"Written $path")
    }
  }

  val executor = Executors.newFixedThreadPool(2)

  timed("multiple files") {
    println(
      ParquetSource("./parquettest/*")
        .toFrame(executor)
        .listener(new Listener {
          var count = 0
          override def onNext(row: Row): Unit = {
            count = count + 1
            if (count % 1000 == 0)
              println(count)
          }
        })
        .toSeq
        .map(convertRow)
        .size
    )
  }

  executor.shutdown()

  private def convertRow(row: Row): (String, Vector[Double]) = {
    val word = row.values(0).asInstanceOf[String]
    val vector = row.values(1).asInstanceOf[util.ArrayList[Double]].asScala.toVector
    (word, vector)
  }
}
