package io.eels.component.parquet

import java.util.concurrent.Executors

import com.sksamuel.exts.metrics.Timed
import io.eels.schema._
import io.eels.{Listener, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.ParquetFileReader

import scala.io.Source
import scala.util.Random
import scala.collection.JavaConverters._

object ParquetReadTest extends App with Timed {

  implicit val fs = FileSystem.getLocal(new Configuration)
  ParquetLogMute()

  val text = getClass.getResourceAsStream("/huckleberry_finn.txt")
  val string = Source.fromInputStream(text).mkString

  val schema = StructType(
    Field("word", StringType),
    Field("doubles", ArrayType(DoubleType))
  )

  for (k <- 1 to 10) {
    val rows = string.split(' ').distinct.map { word =>
      Row(schema, Vector(Random.shuffle(List("sam", "ham", "gam")).head, List.fill(15)(Random.nextDouble)))
    }.toSeq

    val path = new Path(s"./parquettest/huck$k.parquet")
    fs.delete(path, false)
    ParquetSink(path).write(rows)
    println(s"Written $path")
  }

  val path = new Path(s"./parquettest/huck1.parquet")
  val status = fs.getFileStatus(path)
  println(status)
  val x = ParquetFileReader.open(new Configuration(), path)

  val cols = x.getFooter.getFileMetaData.getSchema.getColumns

  x.getRowGroups.asScala.foreach { block =>
    val dictreader = x.getDictionaryReader(block)
    val dict = dictreader.readDictionaryPage(cols.asScala.head)
    println(dict.getEncoding)
  }

  val executor = Executors.newFixedThreadPool(2)

  while (true) {
    timed("multiple files") {
      println(
        ParquetSource("./parquettest/*")
          .toFrame(executor)
          .listener(new Listener {
            var count = 0
            override def onNext(row: Row): Unit = {
              count = count + 1
              if (count % 100000 == 0)
                println(count)
            }
          })
          .toSeq
          .size
      )
    }
  }
  executor.shutdown()
}
