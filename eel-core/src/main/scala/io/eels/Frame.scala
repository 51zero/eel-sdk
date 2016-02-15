package io.eels

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import com.sksamuel.scalax.collection.ConcurrentLinkedQueueConcurrentIterator

trait Frame {
  outer =>

  val DefaultBufferSize = 1000

  def schema: FrameSchema

  def buffer: Buffer

  def join(other: Frame): Frame = new Frame {
    override def schema: FrameSchema = outer.schema.join(other.schema)
    override def buffer: Buffer = new Buffer {

      val buffer1 = outer.buffer
      val buffer2 = other.buffer

      override def close(): Unit = {
        buffer1.close()
        buffer2.close()
      }

      override def iterator: Iterator[Row] = new Iterator[Row] {
        val iter1 = buffer1.iterator
        val iter2 = buffer2.iterator
        override def hasNext: Boolean = iter1.hasNext && iter2.hasNext
        override def next(): Row = iter1.next() join iter2.next()
      }
    }
  }

  def takeWhile(pred: Row => Boolean): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.takeWhile(pred)
    }
  }

  def takeWhile(column: String, pred: (String) => Boolean): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.takeWhile(row => pred(row(column)))
    }
  }

  def except(other: Frame): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer1 = outer.buffer
      val buffer2 = other.buffer

      override def close(): Unit = {
        buffer1.close()
        buffer2.close()
      }

      override def iterator: Iterator[Row] = new Iterator[Row] {
        val iter1 = buffer1.iterator
        val iter2 = buffer2.iterator
        override def hasNext: Boolean = iter1.hasNext && iter2.hasNext
        override def next(): Row = iter1.next except iter2.next
      }
    }
  }

  def dropWhile(p: Row => Boolean): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.dropWhile(p)
    }
  }

  def dropWhile(column: String, p: String => Boolean): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.dropWhile(row => p(row(column)))
    }
  }

  /**
    * Returns a new Frame where only each "k" row is retained. Ie, if sample is 2, then on average,
    * every other row will be returned. If sample is 10 then only 10% of rows will be returned.
    * When running concurrently, the rows that are sampled will vary depending on the ordering that the
    * workers pull through the rows. Each iterator (thread) uses its own count for the sample.
    */
  def sample(k: Int): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.grouped(k).map(_.head)
    }
  }

  def addColumn(name: String, defaultValue: String): Frame = new Frame {
    override def schema: FrameSchema = outer.schema.addColumn(name)
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.map(_.addColumn(name, defaultValue))
    }
  }

  def removeColumn(name: String): Frame = new Frame {
    override def schema: FrameSchema = outer.schema.removeColumn(name)
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.map(_.removeColumn(name))
    }
  }

  def renameColumn(nameFrom: String, nameTo: String): Frame = new Frame {
    override def schema: FrameSchema = outer.schema.renameColumn(nameFrom, nameTo)
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.map(_.renameColumn(nameFrom, nameTo))
    }
  }

  def explode(f: Row => Seq[Row]): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.flatMap(f)
    }
  }

  def fill(defaultValue: String): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.map(_.fill(defaultValue))
    }
  }

  def ++(frame: Frame): Frame = union(frame)
  def union(other: Frame): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer1 = outer.buffer
      val buffer2 = other.buffer
      override def close(): Unit = {
        buffer1.close()
        buffer2.close()
      }
      override def iterator: Iterator[Row] = buffer1.iterator ++ buffer2.iterator
    }
  }

  def projection(first: String, rest: String*): Frame = projection(first +: rest)
  def projection(columns: Seq[String]): Frame = new Frame {
    override val schema: FrameSchema = FrameSchema(columns.map(Column.apply).toList)
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      val newColumns = columns.map(Column.apply)
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.map { row =>
        val map = row.columns.map(_.name).zip(row.fields.map(_.value)).toMap
        val fields = newColumns.map(col => Field(map(col.name)))
        Row(newColumns.toList, fields.toList)
      }
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
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.map { row =>
        f(row)
        row
      }
    }
  }

  def collect(pf: PartialFunction[Row, Row]): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.collect(pf)
    }
  }

  def drop(k: Int): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      val count = new AtomicInteger(0)
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.dropWhile(_ => count.getAndIncrement < k)
    }
  }

  def map(f: Row => Row): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.map(f)
    }
  }

  def filterNot(p: Row => Boolean): Frame = filter(str => !p(str))

  def filter(p: Row => Boolean): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.filter(p)
    }
  }

  /**
    * Filters where the given column matches the given predicate.
    */
  def filter(column: String, p: String => Boolean): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.filter(row => p(row(column)))
    }
  }

  def dropNullRows: Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.filterNot(_.hasNullValue)
    }
  }

  // -- actions --
  def size: Plan[Long] = new ToSizePlan(this)

  def toList: ConcurrentPlan[List[Row]] = new ToListPlan(this)

  def forall(p: (Row) => Boolean): Plan[Boolean] = new ForallPlan(this, p)

  def to(sink: Sink): ConcurrentPlan[Long] = new SinkPlan(sink, this)

  def exists(p: (Row) => Boolean): Plan[Boolean] = new ExistsPlan(this, p)

  def find(p: (Row) => Boolean): Plan[Option[Row]] = new FindPlan(this, p)

  def head: Plan[Option[Row]] = new HeadPlan(this)
}

object Frame {

  import scala.collection.JavaConverters._

  def apply(first: Map[String, String], rest: Map[String, String]*): Frame = new Frame {
    override lazy val schema: FrameSchema = FrameSchema(first.keys.map(Column.apply).toList)
    override def buffer: Buffer = new Buffer {
      val queue = new ConcurrentLinkedQueue[Row]((first +: rest).map(map => Row(map)).asJava)
      override def close(): Unit = ()
      override def iterator: Iterator[Row] = ConcurrentLinkedQueueConcurrentIterator(queue)
    }
  }

  def apply(first: Row, rest: Row*): Frame = apply(first +: rest)
  def apply(rows: Seq[Row]): Frame = new Frame {
    require(rows.nonEmpty)
    override lazy val schema: FrameSchema = FrameSchema(rows.head.columns)
    override def buffer: Buffer = new Buffer {
      val queue = new ConcurrentLinkedQueue[Row](rows.asJava)
      override def close(): Unit = ()
      override def iterator: Iterator[Row] = ConcurrentLinkedQueueConcurrentIterator(queue)
    }
  }
}