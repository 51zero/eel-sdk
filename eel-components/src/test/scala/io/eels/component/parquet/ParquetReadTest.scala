package io.eels.component.parquet

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent._
import java.util.function.Consumer

import com.sksamuel.exts.concurrent.ExecutorImplicits._
import com.sksamuel.exts.metrics.Timed
import io.eels.schema._
import io.eels.{FilePattern, Row}
import io.reactivex.disposables.Disposable
import io.reactivex.{Observable, ObservableEmitter, ObservableOnSubscribe, Observer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import reactor.core.publisher.{Flux, FluxSink}
import reactor.core.scheduler.Schedulers

import scala.io.Source
import scala.util.control.NonFatal

object ParquetReadTest extends App with Timed {

  implicit val conf = new Configuration()
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

    timed("multiple files via part stream on executor") {

      val counter = new AtomicLong(0)
      val executor = Executors.newFixedThreadPool(8)
      val parts = ParquetSource("./parquettest/*").parts2().foreach { part =>
        executor.submit {
          part.stream().foreach(_ => counter.incrementAndGet)
        }
      }
      executor.shutdown()
      executor.awaitTermination(1, TimeUnit.DAYS)
      println(counter.get())
    }

    timed("multiple files via part stream into blocking queue on executor") {

      val counter = new AtomicLong(0)
      val executor = Executors.newFixedThreadPool(8)
      val queue = new LinkedBlockingQueue[List[Row]](1000)

      val parts = ParquetSource("./parquettest/*").parts2()
      val latch = new CountDownLatch(parts.size)

      parts.foreach { part =>
        executor.submit {
          part.stream().foreach(queue.put)
          latch.countDown()
        }
      }

      val s = List(Row.Sentinel)

      executor.submit {
        latch.await()
        queue.put(s)
      }

      Iterator.continually(queue.take).takeWhile(_ != s).foreach { rows =>
        counter.addAndGet(rows.size)
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

      val sched = io.reactivex.schedulers.Schedulers.io

      def observer[T] = new Observer[T] {
        override def onError(e: Throwable): Unit = latch.countDown()
        override def onSubscribe(d: Disposable): Unit = ()
        override def onComplete(): Unit = latch.countDown()
        override def onNext(value: T): Unit = counter.incrementAndGet()
      }

      obs.foreach { ob =>
        ob.subscribeOn(sched).subscribe(observer[Row])
      }
      latch.await()
      println(counter.get())
    }

    timed("observable from a blocking queue") {

      val executor = Executors.newFixedThreadPool(8)
      val queue = new LinkedBlockingQueue[List[Row]](1000)

      val paths = FilePattern(new Path("./parquettest/*")).toPaths()
      val latch = new CountDownLatch(paths.size)

      paths.foreach { path =>
        executor.submit {
          try {
            val reader = ParquetReaderFn(path, None, None)
            val iter = ParquetRowIterator(reader).grouped(100).withPartial(true)
            while (iter.hasNext) {
              queue.put(iter.next)
            }
            latch.countDown()
          } catch {
            case e: Throwable => println(e)
          }
        }
      }

      executor.submit {
        latch.await()
        queue.put(List(Row.Sentinel))
      }

      val counter = new AtomicLong(0)

      val obs = Observable.create(new ObservableOnSubscribe[Row] {
        override def subscribe(e: ObservableEmitter[Row]): Unit = {
          Iterator.continually(queue.take).takeWhile(_ != List(Row.Sentinel)).foreach { rows =>
            rows.foreach { row =>
              e.onNext(row)
              counter.incrementAndGet()
            }
          }
          queue.put(List(Row.Sentinel))
          e.onComplete()
        }
      })

      val latch2 = new CountDownLatch(1)

      def observer[T] = new Observer[T] {
        override def onError(e: Throwable): Unit = latch2.countDown()
        override def onSubscribe(d: Disposable): Unit = ()
        override def onComplete(): Unit = latch2.countDown()
        override def onNext(value: T): Unit = counter.incrementAndGet()
      }

      obs.subscribeOn(io.reactivex.schedulers.Schedulers.single()).subscribe(observer[Row])

      executor.shutdown()
      latch2.await()
      println(counter.get())
    }

    timed("flux into a blocking queue") {

      val queue = new LinkedBlockingQueue[Row](1000)
      val sched = Schedulers.newParallel("nMQWSDFDSF", 8)

      val parts = ParquetSource(FilePattern(new Path("./parquettest/*"))).parts()
      val latch = new CountDownLatch(parts.size)

      parts.foreach { part =>
        part.data.subscribeOn(sched).subscribe(new Consumer[Row] {
          override def accept(row: Row): Unit = {
            queue.put(row)
          }
        }, new Consumer[Throwable] {
          override def accept(t: Throwable): Unit = println(t)
        }, new Runnable {
          override def run(): Unit = latch.countDown()
        })
      }

      new Thread(new Runnable {
        override def run(): Unit = {
          latch.await()
          queue.put(Row.Sentinel)
        }
      }).start()

      val counter = new AtomicLong(0)

      val latch2 = new CountDownLatch(1)

      Flux.create(new Consumer[FluxSink[Row]] {
        override def accept(sink: FluxSink[Row]): Unit = {
          var row = queue.take
          while (!sink.isCancelled && row != Row.Sentinel) {
            sink.next(row)
            row = queue.take()
          }
          queue.put(Row.Sentinel)
          sink.complete()
        }
      }, FluxSink.OverflowStrategy.BUFFER).subscribeOn(Schedulers.single()).subscribe(new Consumer[Row] {
        override def accept(t: Row): Unit = {
          counter.incrementAndGet()
        }
      }, new Consumer[Throwable] {
        override def accept(t: Throwable): Unit = println(t)
      }, new Runnable {
        override def run(): Unit = latch2.countDown()
      })

      latch2.await()
      println(counter.get())
    }

    timed("each part via an Observable merge") {

      val obs = FilePattern(new Path("./parquettest/*")).toPaths().map { path =>
        Observable.create(new ObservableOnSubscribe[List[Row]] {
          override def subscribe(e: ObservableEmitter[List[Row]]): Unit = {
            val reader = ParquetReaderFn(path, None, None)
            try {
              val iter = ParquetRowIterator(reader).grouped(100).withPartial(true)
              while (!e.isDisposed && iter.hasNext) {
                e.onNext(iter.next)
              }
              e.onComplete()
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
      val latch = new CountDownLatch(1)

      import scala.collection.JavaConverters._

      val sched = io.reactivex.schedulers.Schedulers.io

      val o = obs.map(_.subscribeOn(sched))
      Observable.merge(o.asJava).subscribe(new Observer[List[Row]] {
        override def onError(e: Throwable): Unit = ???
        override def onSubscribe(d: Disposable): Unit = ()
        override def onComplete(): Unit = latch.countDown()
        override def onNext(value: List[Row]): Unit = {
          value.foreach(_ => counter.incrementAndGet)
          ()
        }
      })

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