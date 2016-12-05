package io.eels.component.parquet

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.function.Consumer

import com.sksamuel.exts.concurrent.ExecutorImplicits._
import com.sksamuel.exts.metrics.Timed
import io.eels.schema._
import io.eels.{FilePattern, Row}
import io.reactivex.disposables.Disposable
import io.reactivex.{Observable, ObservableEmitter, ObservableOnSubscribe, Observer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers

import scala.io.Source
import scala.util.control.NonFatal

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


  while (true) {

    timed("multiple files via scala iterator") {
      val counter = new AtomicLong(0)
      FilePattern(new Path("./parquettest/*")).toPaths().foreach { path =>
        val reader = ParquetReaderFn(path, None, None)
        val iter = ParquetRowIterator(reader)
        iter.foreach { row =>
          counter.incrementAndGet()
        }
      }
      println(counter.get())
    }

    timed("multiple files via executor") {

      val counter = new AtomicLong(0)
      val executor = Executors.newFixedThreadPool(8)
      FilePattern(new Path("./parquettest/*")).toPaths().foreach { path =>
        executor.submit {
          try {
            val reader = ParquetReaderFn(path, None, None)
            val iter = ParquetRowIterator(reader)
            iter.foreach { row =>
              counter.incrementAndGet()
            }
            //   println("Part completed " + Thread.currentThread)
          } catch {
            case e: Throwable => println(e)
          }
        }
      }
      executor.shutdown()
      executor.awaitTermination(1, TimeUnit.DAYS)
      println(counter.get())
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
            //  println("Part completed " + Thread.currentThread)
            latch.countDown()
          }
        })
      }
      latch.await()
      println(counter.get())
    }

    timed("each part via a Flux merge") {
      val par = Schedulers.newParallel("par", 8)
      val counter = new AtomicLong(0)
      val latch = new CountDownLatch(1)
      Flux.merge(1000,
        ParquetSource("./parquettest/*").parts.map(_.data.subscribeOn(par)): _*
      ).subscribeOn(Schedulers.single()).subscribe(new Consumer[Row] {
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
      latch.await()
      println(counter.get())
    }

    timed("each part via an Observable") {

      val obs = FilePattern(new Path("./parquettest/*")).toPaths().map { path =>
        Observable.create(new ObservableOnSubscribe[Row] {
          override def subscribe(e: ObservableEmitter[Row]): Unit = {
            val reader = ParquetReaderFn(path, None, None)
            try {
              val iter = ParquetRowIterator(reader)
              while (!e.isDisposed && iter.hasNext) {
                e.onNext(iter.next)
              }
              e.onComplete()
              //    logger.debug(s"Parquet reader completed on thread " + Thread.currentThread)
            } catch {
              case NonFatal(error) =>
                logger.warn("Could not read file", error)
                e.onError(error)
            } finally {
              reader.close()
            }
          }
        })
      }

      val counter = new AtomicLong(0)
      val latch = new CountDownLatch(obs.size)

      def observer[T] = new Observer[T] {
        override def onError(e: Throwable): Unit = latch.countDown()
        override def onSubscribe(d: Disposable): Unit = ()
        override def onComplete(): Unit = latch.countDown()
        override def onNext(value: T): Unit = counter.incrementAndGet()
      }

      obs.foreach { ob =>
        ob.subscribeOn(io.reactivex.schedulers.Schedulers.io()).subscribe(observer[Row])
      }
      latch.await()
      println(counter.get())
    }


    timed("multiple files via parquet source") {
      val executor = Executors.newFixedThreadPool(8)
      println(
        ParquetSource("./parquettest/*")
          .toFrame(executor)
          .size()
      )
      executor.shutdown()
      executor.awaitTermination(1, TimeUnit.DAYS)
    }
  }
}