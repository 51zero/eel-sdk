package io.eels.datastream

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.LongAdder

import com.sksamuel.exts.Logging
import io.eels.{Row, Sink}
import io.reactivex.schedulers.Schedulers
import org.reactivestreams.{Subscriber, Subscription}

case class SinkAction(ds: DataStream, sink: Sink, parallelism: Int) extends Logging {

  def execute(): Long = {

    val schema = ds.schema
    val total = new LongAdder
    val latch = new CountDownLatch(parallelism)

    // we open up a separate output stream for each flow
    val streams = sink.open(schema, parallelism)

    val subscribers = streams.map { stream =>
      new Subscriber[Row] {
        override def onSubscribe(s: Subscription): Unit = s.request(Long.MaxValue)
        override def onError(t: Throwable): Unit = {
          logger.error(s"Stream error", t)
          stream.close()
          latch.countDown()
        }
        override def onComplete(): Unit = {
          logger.info(s"Stream completed")
          stream.close()
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
