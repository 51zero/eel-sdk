package io.eels.component

import com.sksamuel.exts.Logging
import io.eels.Row
import io.reactivex.disposables.Disposable
import io.reactivex.{BackpressureStrategy, Flowable, FlowableEmitter, FlowableOnSubscribe}

import scala.util.control.NonFatal

object FlowableIterator extends Logging {

  def apply(iterator: Iterator[Row]): Flowable[Row] = apply(iterator, new Disposable {
    override def isDisposed: Boolean = false
    override def dispose(): Unit = ()
  })

  def apply(iterator: Iterator[Row], disposefn: () => Unit): Flowable[Row] = apply(iterator, new Disposable {
    override def isDisposed: Boolean = false
    override def dispose(): Unit = disposefn()
  })

  def apply(iterator: Iterator[Row], disposable: Disposable): Flowable[Row] = {
    Flowable.create(new FlowableOnSubscribe[Row] {
      override def subscribe(e: FlowableEmitter[Row]): Unit = {
        try {
          e.setDisposable(disposable)
          iterator.foreach(e.onNext)
          e.onComplete()
        } catch {
          case NonFatal(t) => e.onError(t)
        } finally {
          disposable.dispose()
        }
      }
    }, BackpressureStrategy.BUFFER)
  }
}
