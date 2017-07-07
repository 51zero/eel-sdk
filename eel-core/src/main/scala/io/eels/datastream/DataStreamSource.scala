package io.eels.datastream

import java.util.concurrent.Executors

import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Flow, Listener, NoopListener, Row, Source}
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

  override def publisher: Publisher[Seq[Row]] = new Publisher[Seq[Row]] {
    override def subscribe(s: Subscriber[Seq[Row]]): Unit = {
      val flows = source.parts.map(_.open2)
      val e = Executors.newSingleThreadExecutor()
      e.submit(new Runnable {
        override def run(): Unit = {
          try {
            Flow.coalesce(flows, Executors.newCachedThreadPool).iterator.foreach { chunk =>
              s.next(chunk)
            }
            s.completed()
          } catch {
            case t: Throwable => s.error(t)
          }
        }
      })
      e.shutdown
    }
  }
}

trait Publisher[T] {
  def subscribe(subscriber: Subscriber[T])
}

trait Subscription {
  def cancel()
}

trait Subscriber[T] {
  def started(s: Subscription)
  def completed()
  def error(t: Throwable)
  def next(t: T)
}

class DelegateSubscriber[T](delegate: Subscriber[T]) extends Subscriber[T] {
  override def started(s: Subscription): Unit = delegate.started(s)
  override def completed(): Unit = delegate.completed()
  override def error(t: Throwable): Unit = delegate.error(t)
  override def next(t: T): Unit = delegate.next(t)
}