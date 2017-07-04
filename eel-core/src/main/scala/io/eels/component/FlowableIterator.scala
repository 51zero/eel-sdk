package io.eels.component

import com.sksamuel.exts.Logging
import io.eels.Row
import io.reactivex.disposables.Disposable
import io.reactivex.{BackpressureStrategy, Flowable, FlowableEmitter, FlowableOnSubscribe}

import scala.language.implicitConversions
import scala.util.control.NonFatal

object FlowableIterator extends Logging {

  implicit def fnToDisposable(fn: () => Unit): Disposable = new Disposable {
    override def isDisposed: Boolean = false
    override def dispose(): Unit = fn()
  }

  def process(iter: Iterator[Row], disposable: Disposable, e: FlowableEmitter[Row]): Unit = {
    try {
      e.setDisposable(disposable)
      iter.foreach(e.onNext)
      e.onComplete()
    } catch {
      case NonFatal(t) => e.onError(t)
    } finally {
      disposable.dispose()
    }
  }

  def create(thunk: => (Iterator[Row], () => Unit)): Flowable[Row] = {
    Flowable.create(new FlowableOnSubscribe[Row] {
      override def subscribe(e: FlowableEmitter[Row]): Unit = {
        val (iter, disposefn) = thunk
        process(iter, disposefn, e)
      }
    }, BackpressureStrategy.BUFFER)
  }

  def apply(iterator: Iterator[Row]): Flowable[Row] = apply(iterator, new Disposable {
    override def isDisposed: Boolean = false
    override def dispose(): Unit = ()
  })

  def apply(iterator: Iterator[Row], disposefn: () => Unit): Flowable[Row] = apply(iterator, disposefn: Disposable)

  def apply(iterator: Iterator[Row], disposable: Disposable): Flowable[Row] = {
    Flowable.create(new FlowableOnSubscribe[Row] {
      override def subscribe(e: FlowableEmitter[Row]): Unit = {
        process(iterator, disposable, e)
      }
    }, BackpressureStrategy.BUFFER)
  }
}
