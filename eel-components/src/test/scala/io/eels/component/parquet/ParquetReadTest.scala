package io.eels.component.parquet

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent._
import java.util.function.Consumer

import com.sksamuel.exts.concurrent.ExecutorImplicits._
import com.sksamuel.exts.metrics.Timed
import io.eels.schema._
import io.eels.{FilePattern, Row, SourceFrame}
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

  val source = ParquetSource("./parquettest/*")

  while (true) {

    timed("multiple files via part stream into blocking queue on executor") {

      val counter = new AtomicLong(0)
      val executor = Executors.newFixedThreadPool(8)
      val queue = new LinkedBlockingQueue[List[Row]](1000)

      val parts = source.parts()
      val latch = new CountDownLatch(parts.size)

      parts.foreach { part =>
        executor.submit {
          part.iterator().foreach(queue.put)
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

    timed("using parts2 using flux count") {
      val count = new SourceFrame(source).count()
      println(count)
    }

    timed("using parts2 using rows 2") {

      val counter = new AtomicLong(0)

      new SourceFrame(source).rows2.foreach { rows =>
        counter.addAndGet(rows.size)
      }

      println(counter.get())
    }

    timed("using new parts2 using iterator") {

      val counter = new AtomicLong(0)

      val frame = new SourceFrame(source)
      val latch = new CountDownLatch(1)
      new Thread(new Runnable {
        override def run(): Unit = {
          val iter = frame.rows2
          while (iter.hasNext) {
            val rows = iter.next()
            counter.addAndGet(rows.size)
          }
          latch.countDown()
        }
      }).start()
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
        source
          .toFrame(executor)
          .size()
      )
      executor.shutdown()
      executor.awaitTermination(1, TimeUnit.DAYS)
    }
  }
}