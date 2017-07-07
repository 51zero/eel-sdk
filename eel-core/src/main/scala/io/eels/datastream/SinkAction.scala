package io.eels.datastream

import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors}

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

  import scala.collection.JavaConverters._

  def execute(): Long = {

    val schema = ds.schema
    val total = new LongAdder
    val latch = new CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(parallelism)
    val streams = new ConcurrentLinkedQueue(sink.open(schema, parallelism).asJava)

    class OutputWriterTask(chunk: Seq[Row]) extends Runnable {

      override def run(): Unit = {
        val stream = streams.poll
        chunk.foreach(stream.write)
        streams.add(stream)
      }
    }

    ds.publisher.subscribe(new Subscriber[Seq[Row]] {
      override def next(t: Seq[Row]): Unit = {
        executor.execute(new OutputWriterTask(t))
      }

      override def started(s: Subscription): Unit = ()

      override def completed(): Unit = {
        latch.countDown()
      }

      override def error(t: Throwable): Unit = {
        latch.countDown()
      }

    })

    latch.await()
    streams.asScala.foreach(_.close)
    total.sum()
  }
}