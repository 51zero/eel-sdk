package io.eels.datastream

import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{CountDownLatch, Executors, LinkedBlockingQueue, TimeUnit}

import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import com.sksamuel.exts.{Logging, TryOrLog}
import io.eels.{Row, Sink}
import io.reactivex.schedulers.Schedulers

case class SinkAction(ds: DataStream, sink: Sink, parallelism: Int) extends Logging {

  def execute(): Long = {

    val schema = ds.schema
    val total = new LongAdder
    val latch = new CountDownLatch(parallelism)

    // we open up a separate output stream for each flow
    val streams = sink.open(schema, parallelism)

    val subscribers = streams.zipWithIndex.map { case (stream, index) =>
      new org.reactivestreams.Subscriber[Row] {

        override def onSubscribe(s: org.reactivestreams.Subscription): Unit = {
          logger.info(s"Starting output stream $index")
          s.request(Long.MaxValue)
        }

        override def onError(t: Throwable): Unit = {
          logger.error(s"Stream $index error", t)
          TryOrLog {
            stream.close()
          }
          latch.countDown()
        }

        override def onComplete(): Unit = {
          logger.info(s"Stream $index has completed")
          TryOrLog {
            stream.close()
          }
          latch.countDown()
        }

        override def onNext(row: Row): Unit = {
          stream.write(row)
        }
      }
    }

    ds.flowable.parallel(parallelism)
      .runOn(Schedulers.io)
      .subscribe(subscribers.toArray)

    latch.await()
    total.sum()
  }
}

case class SinkAction2(ds: DataStream2, sink: Sink, parallelism: Int) extends Logging {

  def execute(): Long = {

    val schema = ds.schema
    val total = new LongAdder

    val queue = new LinkedBlockingQueue[Seq[Row]](1000)
    val executor = Executors.newFixedThreadPool(parallelism)

    sink.open(schema, parallelism).zipWithIndex.foreach { case (stream, k) =>
      executor.submit(new Runnable {
        logger.debug(s"Starting output stream $k")
        override def run(): Unit = {
          try {
            BlockingQueueConcurrentIterator(queue, Nil).foreach { chunk =>
              chunk.foreach(stream.write)
            }
          } catch {
            case t: Throwable => logger.error("Error writing out", t)
          } finally {
            logger.debug(s"Closing output stream $k")
            stream.close()
          }
        }
      })
    }

    ds.subscribe(new Subscriber[Seq[Row]] {
      override def started(s: Cancellable): Unit = ()
      override def next(t: Seq[Row]): Unit = {
        queue.put(t)
      }
      override def completed(): Unit = queue.put(Nil)
      override def error(t: Throwable): Unit = {
        queue.put(Nil)
        throw t
      }
    })

    // at this point, the subscriber has returned, and now we need to wait until the
    // queue has been emptied by the io threads
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.DAYS)

    total.sum()
  }
}