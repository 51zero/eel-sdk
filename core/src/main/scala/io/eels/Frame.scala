package io.eels

import java.util.concurrent.{TimeUnit, CountDownLatch, ArrayBlockingQueue, BlockingQueue, LinkedBlockingQueue}

import com.sksamuel.scalax.collection.BlockingQueueConcurrentIterator

import scala.concurrent.{ExecutionContext, Future}

trait Frame {
  outer =>

  def buffer(workers: Int)(implicit executor: ExecutionContext): Buffer

  def schema: FrameSchema

  def join(other: Frame): Frame = new Frame {
    override def schema: FrameSchema = outer.schema.join(other.schema)
    override def buffer(workers: Int)(implicit executor: ExecutionContext): Buffer = new BlockingQueueBuffer {

      val buffer1 = outer.buffer(workers)
      val buffer2 = other.buffer(workers)

      override def close(): Unit = {
        buffer1.close()
        buffer2.close()
      }

      val latch = new CountDownLatch(workers)

      for ( k <- 1 to workers ) Future {
        val iter1 = buffer1.iterator
        val iter2 = buffer2.iterator
        new Iterator[Row] {
          override def hasNext: Boolean = iter1.hasNext && iter2.hasNext
          override def next(): Row = iter1.next() join iter2.next()
        } foreach queue.put
        latch.countDown()
      }

      latch.await(1, TimeUnit.HOURS)
      queue.put(Row.Sentinel)
    }
  }

  /**
    * Returns a new Frame where only each "step" row is retained. Ie, if step is 2 then rows 1,3,5,7 will be
    * retainined and if step was 10, then 1,11,21,31 etc.
    */
  def step(k: Int): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer(workers: Int)(implicit executor: ExecutionContext): Buffer = ???
  }

  def addColumn(name: String, defaultValue: String): Frame = new Frame {
    override def schema: FrameSchema = outer.schema.addColumn(name)
    override def buffer(workers: Int)(implicit executor: ExecutionContext): Buffer = new BlockingQueueBuffer {
      val buffer = outer.buffer(workers)
      override def close(): Unit = buffer.close()
      val latch = new CountDownLatch(workers)
      for ( k <- 1 to workers ) Future {
        buffer.iterator.map(_.addColumn(name, defaultValue)).foreach(queue.put)
        latch.countDown()
      }
      latch.await(1, TimeUnit.HOURS)
      queue.put(Row.Sentinel)
    }
  }

  def removeColumn(name: String): Frame = new Frame {
    override def schema: FrameSchema = outer.schema.removeColumn(name)
    override def buffer(workers: Int)(implicit executor: ExecutionContext): Buffer = new BlockingQueueBuffer {
      val buffer = outer.buffer(workers)
      override def close(): Unit = buffer.close()
      val latch = new CountDownLatch(workers)
      for ( k <- 1 to workers ) Future {
        buffer.iterator.map(_.removeColumn(name)).foreach(queue.put)
        latch.countDown()
      }
      latch.await(1, TimeUnit.HOURS)
      queue.put(Row.Sentinel)
    }
  }

  def ++(frame: Frame): Frame = union(frame)
  def union(other: Frame): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer(workers: Int)(implicit executor: ExecutionContext): Buffer = new BlockingQueueBuffer {
      val buffer1 = outer.buffer(workers)
      val buffer2 = other.buffer(workers)
      val latch = new CountDownLatch(workers)
      for ( k <- 1 to workers ) Future {
        (buffer1.iterator ++ buffer2.iterator).foreach(queue.put)
        latch.countDown()
      }
      latch.await(1, TimeUnit.HOURS)
      queue.put(Row.Sentinel)
      override def close(): Unit = {
        buffer1.close()
        buffer2.close()
      }
    }
  }

  def projection(first: String, rest: String*): Frame = projection(first +: rest)
  def projection(columns: Seq[String]): Frame = new Frame {
    override val schema: FrameSchema = FrameSchema(columns.map(Column.apply).toList)
    override def buffer(workers: Int)(implicit executor: ExecutionContext): Buffer = new BlockingQueueBuffer {
      val buffer = outer.buffer(workers)
      val newColumns = columns.map(Column.apply)
      override def close(): Unit = buffer.close()
      val latch = new CountDownLatch(workers)
      for ( k <- 1 to workers ) Future {
        buffer.iterator.map { row =>
          val map = row.columns.map(_.name).zip(row.fields.map(_.value)).toMap
          val fields = newColumns.map(col => Field(map(col.name)))
          Row(newColumns.toList, fields.toList)
        }.foreach(queue.put)
        latch.countDown()
      }
      latch.await(1, TimeUnit.HOURS)
      queue.put(Row.Sentinel)
    }
  }

  /**
    * Execute a side effect function for every row in the frame, returning the same Frame.
    *
    * @param f the function to execute
    * @return this frame, to allow for builder style chaining
    */
  def foreach[U](f: (Row) => U): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer(workers: Int)(implicit executor: ExecutionContext): Buffer = new BlockingQueueBuffer {
      val buffer = outer.buffer(workers)
      override def close(): Unit = buffer.close()
      val latch = new CountDownLatch(workers)
      for ( k <- 1 to workers ) Future {
        buffer.iterator.foreach { row =>
          f(row)
          queue.put(row)
        }
        latch.countDown()
      }
      latch.await(1, TimeUnit.HOURS)
      queue.put(Row.Sentinel)
    }
  }

  def collect(pf: PartialFunction[Row, Row]): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer(workers: Int)(implicit executor: ExecutionContext): Buffer = new BlockingQueueBuffer {
      val buffer = outer.buffer(workers)
      override def close(): Unit = buffer.close()
      val latch = new CountDownLatch(workers)
      for ( k <- 1 to workers ) Future {
        buffer.iterator.collect(pf).foreach(queue.put)
        latch.countDown()
      }
      latch.await(1, TimeUnit.HOURS)
      queue.put(Row.Sentinel)
    }
  }

  /**
    * Single worker
    */
  def drop(k: Int): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer(workers: Int)(implicit executor: ExecutionContext): Buffer = new BlockingQueueBuffer {
      val buffer = outer.buffer(workers)
      override def close(): Unit = buffer.close()
      buffer.iterator.drop(k).foreach(queue.put)
      queue.put(Row.Sentinel)
    }
  }

  def map(f: Row => Row): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer(workers: Int)(implicit executor: ExecutionContext): Buffer = new BlockingQueueBuffer {
      val buffer = outer.buffer(workers)
      override def close(): Unit = buffer.close()
      val latch = new CountDownLatch(workers)
      for ( k <- 1 to workers ) Future {
        buffer.iterator.map(f).foreach(queue.put)
        latch.countDown()
      }
      latch.await(1, TimeUnit.HOURS)
      queue.put(Row.Sentinel)
    }
  }

  def filterNot(p: Row => Boolean): Frame = filter(str => !p(str))

  def filter(p: Row => Boolean): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer(workers: Int)(implicit executor: ExecutionContext): Buffer = new BlockingQueueBuffer {
      val buffer = outer.buffer(workers)
      override def close(): Unit = buffer.close()
      val latch = new CountDownLatch(workers)
      for ( k <- 1 to workers ) Future {
        buffer.iterator.filter(p).foreach(queue.put)
        latch.countDown()
      }
      latch.await(1, TimeUnit.HOURS)
      queue.put(Row.Sentinel)
    }
  }

  /**
    * Filters where the given column matches the given predicate.
    */
  def filter(column: String, p: String => Boolean): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer(workers: Int)(implicit executor: ExecutionContext): Buffer = new BlockingQueueBuffer {
      val buffer = outer.buffer(workers)
      override def close(): Unit = buffer.close()
      val latch = new CountDownLatch(workers)
      for ( k <- 1 to workers ) Future {
        buffer.iterator.filter(row => p(row(column))).foreach(queue.put)
        latch.countDown()
      }
      latch.await(1, TimeUnit.HOURS)
      queue.put(Row.Sentinel)
    }
  }

  // -- actions --
  def size: Plan[Long] = new ToSizePlan(this)

  def toList: Plan[List[Row]] = new ToListPlan(this)

  def forall(p: (Row) => Boolean): Plan[Boolean] = new ForallPlan(this, p)

  def to(sink: Sink): ConcurrentPlan[Long] = new SinkPlan(sink, this)

  def exists(p: (Row) => Boolean): Plan[Boolean] = new ExistsPlan(this, p)

  def find(p: (Row) => Boolean): Plan[Option[Row]] = new FindPlan(this, p)

  def head: Plan[Option[Row]] = new HeadPlan(this)
}

object Frame {

  def apply(first: Row, rest: Row*): Frame = new Frame {
    override lazy val schema: FrameSchema = FrameSchema(first.columns)
    override def buffer(workers: Int)(implicit executor: ExecutionContext): Buffer = new BlockingQueueBuffer {
      (first +: rest).foreach(queue.put)
      queue.put(Row.Sentinel)
      override def close(): Unit = ()
    }
  }
}

trait Buffer {
  def close(): Unit
  def iterator: Iterator[Row]
}

trait BlockingQueueBuffer extends Buffer {
  val queue = new ArrayBlockingQueue[Row](100)
  override def iterator: Iterator[Row] = BlockingQueueConcurrentIterator(queue, Row.Sentinel)
}