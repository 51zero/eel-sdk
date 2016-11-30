package io.eels.component.parquet

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, Executors}
import java.util.function.Consumer

import com.sksamuel.exts.metrics.Timed
import io.eels.schema._
import io.eels.{FilePattern, Listener, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import reactor.core.scheduler.Schedulers

import scala.io.Source

object ParquetReadTest extends App with Timed {

  implicit val fs = FileSystem.getLocal(new Configuration)
  ParquetLogMute()

  val text = getClass.getResourceAsStream("/huckleberry_finn.txt")
  val string = Source.fromInputStream(text).mkString

  val schema = StructType(
    Field("word", StringType),
    Field("doubles", ArrayType(DoubleType))
  )

  //  for (k <- 1 to 50) {
  //    val rows = string.split(' ').distinct.map { word =>
  //      Row(schema, Vector(word, List.fill(15)(Random.nextDouble)))
  //    }.toSeq
  //
  //    val path = new Path(s"./parquettest/huck$k.parquet")
  //    fs.delete(path, false)
  //    ParquetSink(path).write(rows)
  //    println(s"Written $path")
  //  }

  val executor = Executors.newFixedThreadPool(2)

  while (true) {
    timed("multiple files via scala iterator") {
      val count = FilePattern(new Path("./parquettest")).toPaths().map { path =>
        val reader = ParquetReaderFn(path, None, None)
        val iter = ParquetRowIterator(reader)
        iter.foldLeft(0L)((counter, row) => {
          if (counter % 100000 == 0) println(counter)
          counter + 1
        })
      }.sum
      println(count)
    }
    timed("each part via a Flux using a par") {
      val parts = ParquetSource("./parquettest/*").parts()
      val par = Schedulers.newParallel("par", 8)
      val counter = new AtomicLong(0)
      val latch = new CountDownLatch(parts.size)
      parts.foreach { part =>
        part.data().subscribeOn(par).subscribe(new Consumer[Row] {
          override def accept(t: Row): Unit = {
            counter.incrementAndGet()
          }
        }, new Consumer[Throwable] {
          override def accept(t: Throwable): Unit = {
            println(t)
            latch.countDown()
          }
        }, new Runnable {
          override def run(): Unit = {
            println("Part completed " + Thread.currentThread)
            latch.countDown()
          }
        })
      }
      latch.await()
      println(counter.get())
    }
    timed("each part via a Flux using an elastic") {
      val parts = ParquetSource("./parquettest/*").parts()
      val elastic = Schedulers.newElastic("elastic")
      val counter = new AtomicLong(0)
      val latch = new CountDownLatch(parts.size)
      parts.foreach { part =>
        part.data().subscribeOn(elastic).subscribe(new Consumer[Row] {
          override def accept(t: Row): Unit = {
            counter.incrementAndGet()
          }
        }, new Consumer[Throwable] {
          override def accept(t: Throwable): Unit = {
            println(t)
            latch.countDown()
          }
        }, new Runnable {
          override def run(): Unit = {
            println("Part completed " + Thread.currentThread)
            latch.countDown()
          }
        })
      }
      latch.await()
      println(counter.get())
    }
    timed("multiple files via parquet source") {
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
