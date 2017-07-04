package io.eels.datastream

import java.util.concurrent.atomic.{AtomicInteger, LongAdder}
import java.util.concurrent.{BlockingQueue, Executors, LinkedBlockingQueue}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Listener, NoopListener, Row, Sink, Source}
import io.reactivex.Flowable
import io.reactivex.schedulers.Schedulers

import scala.collection.JavaConverters._

// an implementation of DataStream that provides a Flowable populated from 1 or more parts
class DataStreamSource(source: Source, listener: Listener = NoopListener) extends DataStream with Using {

  override def schema: StructType = source.schema

  override def flowable: Flowable[Row] = {
    val parts = source.parts()
    if (parts.isEmpty) {
      Flowable.empty()
    } else {
      val flowables = parts.map(_.open.subscribeOn(Schedulers.io))
      Flowable.merge(flowables.asJava).observeOn(Schedulers.computation)
    }
  }
}

class DataStreamSource2(source: Source, listener: Listener = NoopListener) extends DataStream2 with Using {
  self =>

  override def schema: StructType = source.schema

  override def flow: BlockingQueue[Seq[Row]] = {
    val parts = source.parts()
    if (parts.isEmpty) {
      new LinkedBlockingQueue[Seq[Row]]
    } else {
      val executor = Executors.newCachedThreadPool()
      val completed = new AtomicInteger(0)
      val queue = new LinkedBlockingQueue[Seq[Row]](1000)
      parts.foreach { part =>
        executor.execute(new Runnable {
          override def run(): Unit = {
            part.open2().iterator.foreach(queue.put)
            if (completed.incrementAndGet == parts.size) {
              queue.put(Nil)
            }
          }
        })
      }
      executor.shutdown()
      queue
    }
  }

  def listener(_listener: Listener): DataStream2 = new DataStream2 {
    override def schema: StructType = self.schema
    override def flow: BlockingQueue[Seq[Row]] = {
      val queue = new LinkedBlockingQueue[Seq[Row]](1000)
      BlockingQueueConcurrentIterator(self.flow, Nil).foreach { chunk =>
        queue put chunk.map { row =>
          _listener.onNext(row)
          row
        }
      }
      queue.put(Nil)
      queue
    }
  }
}

trait DataStream2 extends Logging {

  def schema: StructType
  def flow: BlockingQueue[Seq[Row]]

  def size: Long = {
    val adder = new LongAdder()
    BlockingQueueConcurrentIterator(flow, Nil).foreach(_.foreach(_ => adder.increment))
    adder.sum()
  }

  def to(sink: Sink): Long = to(sink, 1)
  def to(sink: Sink, parallelism: Int): Long = SinkAction2(this, sink, parallelism).execute()
}