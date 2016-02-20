package io.eels

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import com.sksamuel.scalax.collection.ConcurrentLinkedQueueConcurrentIterator
import io.eels.plan.ToSeqPlan

import scala.concurrent.ExecutionContext

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
        override def next(): Row = iter1.next() ++ iter2.next()
      }
    }
  }

  def replace(from: String, target: Any): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.map(RowUtils.replace(from, target, _))
    }
  }

  def replace(columnName: String, from: String, target: Any): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      val index = outer.schema.indexOf(columnName)
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.map(RowUtils.replace(index, from, target, _))
    }
  }

  def replace(columnName: String, fn: Any => Any): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      val index = outer.schema.indexOf(columnName)
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.map(RowUtils.replaceByFn(index, fn, _))
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

  def takeWhile(columnName: String, p: Any => Boolean): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      val index = outer.schema.indexOf(columnName)
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.takeWhile(row => p(row(index)))
    }
  }

  def except(other: Frame): Frame = new Frame {

    override def schema: FrameSchema = outer.schema.removeColumns(other.schema.columnNames)

    override def buffer: Buffer = new Buffer {
      val buffer1 = outer.buffer
      val buffer2 = other.buffer
      val schema1 = outer.schema
      val schema2 = other.schema

      override def close(): Unit = {
        buffer1.close()
        buffer2.close()
      }

      override def iterator: Iterator[Row] = new Iterator[Row] {
        val iter1 = buffer1.iterator
        val iter2 = buffer2.iterator
        override def hasNext: Boolean = iter1.hasNext && iter2.hasNext
        override def next(): Row = RowUtils.toMap(schema1, iter1.next).--(schema2.columnNames).values.toList
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

  def dropWhile(columnName: String, p: Any => Boolean): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      val index = outer.schema.indexOf(columnName)
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.dropWhile(row => p(row(index)))
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
      override def iterator: Iterator[Row] = buffer.iterator.map(_ :+ defaultValue)
    }
  }

  def removeColumn(columnName: String): Frame = new Frame {
    override def schema: FrameSchema = outer.schema.removeColumn(columnName)
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      val index = outer.schema.indexOf(columnName)
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.map { row =>
        row.slice(0, index) ++ row.slice(index + 1, row.length)
      }
    }
  }

  def renameColumn(nameFrom: String, nameTo: String): Frame = new Frame {
    override def schema: FrameSchema = outer.schema.renameColumn(nameFrom, nameTo)
    override def buffer: Buffer = outer.buffer
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
      override def iterator: Iterator[Row] = buffer.iterator.map(_.map {
        case null => defaultValue
        case other => other
      })
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

  def projectionExpression(expr: String): Frame = projection(expr.split(',').map(_.trim))
  def projection(first: String, rest: String*): Frame = projection(first +: rest)
  def projection(columns: Seq[String]): Frame = new Frame {

    lazy val outerSchema = outer.schema

    override lazy val schema: FrameSchema = {
      val newColumns = columns.map { col =>
        outerSchema.columns.find(_.name == col).getOrElse(sys.error(s"$col is not in the source frame"))
      }
      FrameSchema(newColumns.toList)
    }

    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      // we get a sequence of the indexes of the columns in the original schema, so we can just
      // apply those indexes to the incoming rows
      val indexes = columns.map(outerSchema.indexOf).toVector
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.map(row => indexes.map(row.apply))
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
      override def iterator: Iterator[Row] = buffer.iterator.map {
        row =>
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
  def filter(columnName: String, p: Any => Boolean): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      val index = outer.schema.indexOf(columnName)
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.filter(row => p(row(index)))
    }
  }

  def dropNullRows: Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def buffer: Buffer = new Buffer {
      val buffer = outer.buffer
      override def close(): Unit = buffer.close()
      override def iterator: Iterator[Row] = buffer.iterator.filterNot(_.contains(null))
    }
  }

  // -- actions --
  def size: ConcurrentPlan[Long] = new ToSizePlan(this)

  def fold[A](a: A)(fn: (A, Row) => A): Plan[A] = new FoldPlan(a, fn, this)

  def toSeq(implicit executor: ExecutionContext): Seq[Row] = ToSeqPlan(this)

  def toSet: ConcurrentPlan[scala.collection.mutable.Set[Row]] = new ToSetPlan(this)

  def forall(p: (Row) => Boolean): Plan[Boolean] = new ForallPlan(this, p)

  def to(sink: Sink): ConcurrentPlan[Long] = new SinkPlan(sink, this)

  def exists(p: (Row) => Boolean): Plan[Boolean] = new ExistsPlan(this, p)

  def find(p: (Row) => Boolean): Plan[Option[Row]] = new FindPlan(this, p)

  def head: Plan[Option[Row]] = new HeadPlan(this)
}

object Frame {

  import scala.collection.JavaConverters._

  def apply(_schema: FrameSchema, first: Row, rest: Row*): Frame = apply(_schema, first +: rest)
  def apply(_schema: FrameSchema, rows: Seq[Row]): Frame = new Frame {
    override def buffer: Buffer = new Buffer {
      val queue = new ConcurrentLinkedQueue[Row](rows.asJava)
      override def close(): Unit = ()
      override def iterator: Iterator[Row] = ConcurrentLinkedQueueConcurrentIterator(queue)
    }
    override def schema: FrameSchema = _schema
  }

  def apply(first: Map[String, String], rest: Map[String, String]*): Frame = new Frame {
    override lazy val schema: FrameSchema = FrameSchema(first.keys.map(Column.apply).toList)
    override def buffer: Buffer = new Buffer {
      val queue = new ConcurrentLinkedQueue[Row]((first +: rest).map(_.valuesIterator.toSeq).asJava)
      override def close(): Unit = ()
      override def iterator: Iterator[Row] = ConcurrentLinkedQueueConcurrentIterator(queue)
    }
  }
}