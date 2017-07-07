package io.eels.datastream

import java.util.concurrent.LinkedBlockingQueue

import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Listener, NoopListener, Row, Source}
import io.reactivex.Flowable
import io.reactivex.schedulers.Schedulers

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

// an implementation of DataStream that provides a Flowable populated from 1 or more parts
class DataStreamSource(source: Source, listener: Listener = NoopListener) extends DataStream with Using {

  override def schema: StructType = source.schema

  override def flowable: Flowable[Row] = {
    val parts = source.parts()
    if (parts.isEmpty) {
      Flowable.empty()
    } else {
      try {
        val flowables = parts.map(_.open.subscribeOn(Schedulers.io))
        Flowable.merge(flowables.asJava)
      } catch {
        case NonFatal(e) => Flowable.error(e)
      }
    }
  }
}

class DataStreamSource2(source: Source) extends DataStream2 with Using {

  override def schema: StructType = source.schema

  override def subscribe(s: Subscriber[Seq[Row]]): Unit = {

    val queue = new LinkedBlockingQueue[Seq[Row]](1000)

    source.parts.foreach { part =>
      part.subscribe(new Subscriber[Seq[Row]] {
        override def next(t: Seq[Row]): Unit = queue.put(t)
        override def started(c: Cancellable): Unit = ()
        override def completed(): Unit = queue.put(Nil)
        override def error(t: Throwable): Unit = queue.put(Nil)
      })
    }

    Attempt(s) { sub =>
      BlockingQueueConcurrentIterator(queue, Nil).foreach(sub.next)
    }
  }
}

trait Cancellable {
  def cancel()
}

trait Subscriber[T] {
  def started(s: Cancellable)
  def completed()
  def error(t: Throwable)
  def next(t: T)
}

class DelegateSubscriber[T](delegate: Subscriber[T]) extends Subscriber[T] {
  override def started(c: Cancellable): Unit = delegate.started(c)
  override def completed(): Unit = delegate.completed()
  override def error(t: Throwable): Unit = delegate.error(t)
  override def next(t: T): Unit = delegate.next(t)
}