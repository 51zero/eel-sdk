package io.eels

import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{BlockingQueue, Executor, LinkedBlockingQueue}

import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator

import scala.collection.{GenTraversableOnce, Iterator}

trait Flow {
  self =>

  // delegate methods
  def close(): Unit
  val iterator: Iterator[Row]

  def foreach(f: Row => Any): Unit = self.iterator.foreach(f)

  def map[B](f: Row => Row): Flow = new Flow {
    override def close(): Unit = self.close()
    override val iterator = self.iterator.map(f)
  }

  def flatMap(f: Row => GenTraversableOnce[Row]): Flow = new Flow {
    override def close(): Unit = self.close()
    override val iterator = self.iterator.flatMap(f)
  }

  def filter(p: Row => Boolean): Flow = new Flow {
    override def close(): Unit = self.close()
    override val iterator = self.iterator.filter(p)
  }

  def filterNot(p: Row => Boolean): Flow = filter(!p(_))

  def drop(n: Int): Flow = new Flow {
    override def close(): Unit = self.close()
    override val iterator = self.iterator.drop(n)
  }

  def take(n: Int): Flow = new Flow {
    override def close(): Unit = self.close()
    override val iterator = self.iterator.take(n)
  }

  def dropWhile(p: Row => Boolean): Flow = new Flow {
    override def close(): Unit = self.close()
    override val iterator = self.iterator.dropWhile(p)
  }

  def takeWhile(p: Row => Boolean): Flow = new Flow {
    override def close(): Unit = self.close()
    override val iterator = self.iterator.takeWhile(p)
  }

  def concat(other: Flow): Flow = new Flow {
    override def close(): Unit = {
      self.close()
      other.close()
    }
    override val iterator = self.iterator ++ other.iterator
  }
}

object Flow {

  import com.sksamuel.exts.concurrent.ExecutorImplicits._

  // combine multiple Flows into a single Flow, backed by a blocking queue
  // the executor will be used for the tasks to populate the queue from the input flows
  def coalesce(flows: Seq[Flow], executor: Executor): Flow = {
    val queue = Flow.toQueue(flows, executor)
    val closeable = new Closeable {
      override def close(): Unit = flows.foreach(_.close)
    }
    Flow(closeable, BlockingQueueConcurrentIterator(queue, Row.Sentinel))
  }

  def parallelize(flow: Flow, n: Int, executor: Executor): Seq[Flow] = {
    val queue = new LinkedBlockingQueue[Row](1000)
    executor.execute {
      flow.iterator.foreach(queue.put)
      queue.put(Row.Sentinel)
      ()
    }
    List.fill(n)(Flow(flow.close _, BlockingQueueConcurrentIterator(queue, Row.Sentinel)))
  }

  def toQueue(flows: Seq[Flow], executor: Executor): BlockingQueue[Row] = {
    val queue = new LinkedBlockingQueue[Row](100)
    val completed = new AtomicInteger(0)
    flows.foreach { flow =>
      executor.execute {
        flow.iterator.foreach(queue.put)
        if (completed.incrementAndGet == flows.size) {
          queue.put(Row.Sentinel)
        }
        ()
      }
    }
    queue
  }

  def apply(iter: Iterator[Row]): Flow = new Flow {
    override def close(): Unit = ()
    override val iterator = iter
  }

  def apply(closeable: Closeable, iter: Iterator[Row]): Flow = new Flow {
    override def close(): Unit = closeable.close()
    override val iterator = iter
  }

  def apply(closefn: () => Unit, iter: Iterator[Row]): Flow = new Flow {
    override def close(): Unit = closefn()
    override val iterator = iter
  }

  def empty: Flow = new Flow {
    override def close(): Unit = ()
    override val iterator = Iterator.empty
  }
}
