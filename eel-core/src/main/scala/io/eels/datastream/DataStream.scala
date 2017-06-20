package io.eels.datastream

import java.io.Closeable
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, LongAdder}
import java.util.concurrent.{CountDownLatch, Executors, LinkedBlockingQueue, TimeUnit}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import io.eels.actions.CountAction
import io.eels.schema.{DataType, Field, StringType, StructType}
import io.eels.{CloseableIterator, Listener, NoopListener, Row, Sink}

import scala.concurrent.ExecutionContext
import scala.language.implicitConversions
import scala.util.Try
import scala.util.control.NonFatal

/**
  * A DataStream is kind of like a table of data. It has fields (like columns) and rows of data. Each row
  * has an entry for each field (this may be null depending on the field definition).
  *
  * It is a lazily evaluated data structure. Each operation on a stream will create a new derived stream,
  * but those operations will only occur when a final action is performed.
  *
  * You can create a DataStream from an IO source, such as a Parquet file or a Hive table, or you may
  * create a fully evaluated one from an in memory structure. In the case of the former, the data
  * will only be loaded on demand as an action is performed.
  *
  * A DataStream is split into one or more partitions. Each partition can operate independantly
  * of the others. For example, if you filter a stream, each partition will be filtered seperately,
  * which allows it to be parallelized. If you write out a stream, each partition can be written out
  * to individual files, again allowing parallelization.
  *
  */
trait DataStream extends Logging {
  outer =>

  implicit val executor = ExecutionContext.Implicits.global

  /**
    * Returns the Schema for this stream. This call will not cause a full evaluation, but only
    * the operations required to retrieve a schema will occur. For example, on a stream backed
    * by a JDBC source, an empty resultset will be obtained in order to query the metadata for
    * the database columns.
    */
  def schema: StructType

  private[eels] def partitions: Seq[CloseableIterator[Row]]

  private[eels] def coalesce: CloseableIterator[Row] = {

    val partitions = outer.partitions
    if (partitions.size == 1) {
      partitions.head
    } else {

      val queue = new LinkedBlockingQueue[Row](1000)
      val completed = new AtomicInteger(0)

      partitions.zipWithIndex.foreach { case (partition, index) =>
        executor.execute(new Runnable {
          override def run(): Unit = {
            try {
              logger.info(s"Starting coalesce thread for partition ${index + 1}")
              partition.iterator.foreach(queue.put)
              logger.info(s"Finished coalesce thread for partition ${index + 1}")
            } catch {
              case NonFatal(e) =>
                logger.error("Error running coalesce task", e)
            } finally {
              if (completed.incrementAndGet == partitions.size) {
                logger.info("All coalesce tasks completed, closing downstream queue")
                queue.put(Row.Sentinel)
              }
            }
          }
        })
      }
      CloseableIterator(new Closeable {
        override def close(): Unit = {
          partitions.map(_.closeable).foreach(_.close)
        }
      }, BlockingQueueConcurrentIterator(queue, Row.Sentinel))
    }
  }

  def map(f: Row => Row): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override private[eels] def partitions = outer.partitions.map(_.map(f))
  }

  def filterNot(p: (Row) => Boolean): DataStream = filter { row => !p(row) }

  /**
    * For each row in the stream, filter drops any rows which do not match the predicate.
    */
  def filter(p: Row => Boolean): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    // we can keep each partition as is, and just filter individually
    override def partitions: Seq[CloseableIterator[Row]] = {
      outer.partitions.map(_.filter(p))
    }
  }

  /**
    * Filters where the given field name matches the given predicate.
    */
  def filter(fieldName: String, p: (Any) => Boolean): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override def partitions: Seq[CloseableIterator[Row]] = {
      val index = schema.indexOf(fieldName)
      if (index < 0)
        sys.error(s"Unknown field $fieldName")
      outer.partitions.map(_.filter { row => p(row.values(index)) })
    }
  }

  def projectionExpression(expr: String): DataStream = projection(expr.split(',').map(_.trim()))
  def projection(first: String, rest: String*): DataStream = projection((first +: rest).toList)

  /**
    * Returns a new DataStream which contains the given list of fields from the existing stream.
    */
  def projection(fields: Seq[String]): DataStream = new DataStream {
    override def schema: StructType = outer.schema.projection(fields)
    override private[eels] def partitions = {

      val oldSchema = outer.schema
      val newSchema = schema

      outer.partitions.map { partition =>
        partition.map { row =>
          val values = newSchema.fieldNames().map { name =>
            val k = oldSchema.indexOf(name)
            row.values(k)
          }
          Row(newSchema, values)
        }
      }
    }
  }

  def replaceNullValues(defaultValue: String): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override private[eels] def partitions = {
      outer.partitions.map { partition =>
        partition.map { row =>
          val newValues = row.values.map {
            case null => defaultValue
            case otherwise => otherwise
          }
          Row(row.schema, newValues)
        }
      }
    }
  }

  /**
    * Returns a new DataStream where only each "k" row is retained. Ie, if sample is 2, then on average,
    * every other row will be returned. If sample is 10 then only 10% of rows will be returned.
    * When running concurrently, the rows that are sampled will vary depending on the ordering that the
    * workers pull through the rows. Each partition uses its own couter.
    */
  def sample(k: Int): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override private[eels] def partitions = outer.partitions.map { partition =>
      val counter = new AtomicLong(0)
      partition.filter { row =>
        if (counter.getAndIncrement % k == 0) false
        else true
      }
    }
  }

  /**
    * Joins two streams together, such that the elements of the given frame are appended to the
    * end of this streams. This operation is the same as a concat operation.
    * This results in having numPartitions(a) + numPartitions(b)
    */
  def ++(other: DataStream): DataStream = union(other)
  def union(other: DataStream): DataStream = new DataStream {
    // todo check schemas are compatible
    override def schema: StructType = outer.schema
    override private[eels] def partitions = outer.partitions ++ other.partitions
  }

  /**
    * Returns the same data but with an updated schema. The field that matches
    * the given name will have its datatype set to the given datatype.
    */
  def updateFieldType(fieldName: String, datatype: DataType): DataStream = new DataStream {
    override def schema: StructType = outer.schema.updateFieldType(fieldName, datatype)
    override private[eels] def partitions = {
      val updatedSchema = schema
      outer.partitions.map { part => part.map { row => Row(updatedSchema, row.values) } }
    }
  }

  def updateField(name: String, field: Field): DataStream = new DataStream {
    override def schema: StructType = outer.schema.replaceField(name, field)
    override private[eels] def partitions = {
      val updatedSchema = schema
      outer.partitions.map { partition =>
        partition.map { row => Row(updatedSchema, row.values) }
      }
    }
  }

  /**
    * Returns a new DataStream with the same data as this stream, but where the field names have been sanitized
    * by removing any occurances of the given characters.
    */
  def stripCharsFromFieldNames(chars: Seq[Char]): DataStream = new DataStream {
    override def schema: StructType = outer.schema.stripFromFieldNames(chars)
    override private[eels] def partitions = {
      val updatedschema = schema
      outer.partitions.map { partition =>
        partition.map { row => Row(updatedschema, row.values) }
      }
    }
  }

  /**
    * For each row, the value corresponding to the given fieldName is applied to the function.
    * The result of the function is the new value for that cell.
    */
  def replace(fieldName: String, fn: (Any) => Any): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override private[eels] def partitions = {
      val index = schema.indexOf(fieldName)
      outer.partitions.map { partition =>
        partition.map { row =>
          val newValues = row.values.updated(index, fn(row.values(index)))
          Row(row.schema, newValues)
        }
      }
    }
  }

  /**
    * Replaces any values that match "form" with the value "target".
    * This operation only applies to the field name specified.
    */
  def replace(fieldName: String, from: String, target: Any): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override private[eels] def partitions = {
      val index = schema.indexOf(fieldName)
      outer.partitions.map { partition =>
        partition.map { row =>
          val existing = row.values(index)
          if (existing == from) {
            Row(row.schema, row.values.updated(index, target))
          } else {
            row
          }
        }
      }
    }
  }

  def explode(fn: (Row) => Seq[Row]): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override private[eels] def partitions = outer.partitions.map { partition =>
      partition.flatMap { row => fn(row) }
    }
  }

  def replaceFieldType(from: DataType, toType: DataType): DataStream = new DataStream {
    override def schema: StructType = outer.schema.replaceFieldType(from, toType)
    override private[eels] def partitions = {
      val updatedSchema = schema
      outer.partitions.map { partition =>
        partition.map { row => Row(updatedSchema, row.values) }
      }
    }
  }

  /**
    * Foreach row, any values that match "from" will be replaced with "target".
    * This operation applies to all values for all rows.
    */
  def replace(from: String, target: Any): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override private[eels] def partitions = {
      outer.partitions.map { partition =>
        partition.map { row =>
          val values = row.values.map { value =>
            if (value == from) target else value
          }
          Row(row.schema, values)
        }
      }
    }
  }

  def addFieldIfNotExists(name: String, defaultValue: Any): DataStream = addFieldIfNotExists(Field(name, StringType), defaultValue)
  def addFieldIfNotExists(field: Field, defaultValue: Any): DataStream = {
    val exists = outer.schema.fieldNames().contains(field.name)
    if (exists) this else addField(field, defaultValue)
  }

  /**
    * Returns a new DataStream with the given field added at the end. The value of this field
    * for each Row is specified by the default value. The value must be compatible with the
    * field definition. Eg, an error will occur if the field has type Int and the default
    * value was 1.3
    */
  def addField(field: Field, defaultValue: Any): DataStream = new DataStream {
    override def schema: StructType = outer.schema.addField(field)
    override def partitions: Seq[CloseableIterator[Row]] = {
      val exists = outer.schema.fieldNames().contains(field.name)
      if (exists) sys.error(s"Cannot add field ${field.name} as it already exists")
      val newSchema = schema
      outer.partitions.map { part => part.map(row => Row(newSchema, row.values :+ defaultValue)) }
    }
  }

  /**
    * Returns a new DataStream with the new field of type String added at the end. The value of
    * this field for each Row is specified by the default value.
    */
  def addField(name: String, defaultValue: String): DataStream = addField(Field(name, StringType), defaultValue)

  /**
    * Execute a side effecting function for every row in the stream, returning the same row.
    */
  def foreach[U](fn: (Row) => U): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override def partitions: Seq[CloseableIterator[Row]] = outer.partitions.map(_.map { row =>
      fn(row)
      row
    })
  }

  def removeField(fieldName: String, caseSensitive: Boolean = true): DataStream = new DataStream {
    override def schema: StructType = outer.schema.removeField(fieldName, caseSensitive)
    override private[eels] def partitions = {
      val index = outer.schema.indexOf(fieldName, caseSensitive)
      val newSchema = schema
      outer.partitions.map { partition =>
        partition.map { row =>
          val newValues = row.values.slice(0, index) ++ row.values.slice(index + 1, row.values.size)
          Row(newSchema, newValues)
        }
      }
    }
  }

  /**
    * Combines two frames together such that the fields from this frame are joined with the fields
    * of the given frame. Eg, if this frame has A,B and the given frame has C,D then the result will
    * be A,B,C,D
    *
    * Each stream has different partitions so we'll need to re-partition it to ensure we have an even
    * distribution.
    */
  def join(other: DataStream): DataStream = new DataStream {
    override def schema: StructType = outer.schema.join(other.schema)
    override private[eels] def partitions = {
      val combinedSchema = schema
      val a = outer.coalesce
      val b = other.coalesce
      val closeable = new Closeable {
        override def close(): Unit = {
          a.close()
          b.close()
        }
      }
      val iterator = outer.coalesce.iterator.zip(other.coalesce.iterator).map { case (x, y) =>
        Row(combinedSchema, x.values ++ y.values)
      }
      Seq(CloseableIterator(closeable, iterator))
    }
  }

  def renameField(nameFrom: String, nameTo: String): DataStream = new DataStream {
    override def schema: StructType = outer.schema.renameField(nameFrom, nameTo)
    override private[eels] def partitions = {
      val updatedSchema = schema
      outer.partitions.map { CloseIterator =>
        CloseIterator.map { row => Row(updatedSchema, row.values) }
      }
    }
  }

  def takeWhile(fieldName: String, pred: Any => Boolean): DataStream = takeWhile(row => pred(row.get(fieldName)))
  def takeWhile(pred: Row => Boolean): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override def partitions: Seq[CloseableIterator[Row]] = Seq(outer.coalesce.takeWhile(pred))
  }

  def take(k: Int): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override def partitions: Seq[CloseableIterator[Row]] = Seq(outer.coalesce.take(k))
  }

  /**
    * Returns a new DataStream where k number of rows has been dropped.
    * This operation requires a reshuffle.
    */
  def drop(k: Int): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override def partitions: Seq[CloseableIterator[Row]] = {
      Seq(outer.coalesce.drop(k))
    }
  }

  def dropWhile(p: (Row) => Boolean): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override private[eels] def partitions = Seq(outer.coalesce.dropWhile(p))
  }

  def dropWhile(fieldName: String, pred: (Any) => Boolean): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override private[eels] def partitions = {
      val index = outer.schema.indexOf(fieldName)
      Seq(outer.coalesce.dropWhile { row => pred(row.values(index)) })
    }
  }

  // returns a new DataStream with any rows that contain one or more nulls excluded
  def dropNullRows(): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override private[eels] def partitions = outer.partitions.map { partition => partition.filterNot(_.values.contains(null)) }
  }

  def withLowerCaseSchema(): DataStream = new DataStream {
    private lazy val lowerSchema = outer.schema.toLowerCase()
    override def schema: StructType = lowerSchema
    override def partitions: Seq[CloseableIterator[Row]] = outer.partitions
  }

  // allows aggregations on the entire dataset
  def aggregated(): GroupedDataStream = new GroupedDataStream {
    override def source: DataStream = outer
    override def keyFn: Row => Any = GroupedDataStream.FullDatasetKeyFn
    override def aggregations: Vector[Aggregation] = Vector.empty
  }

  // group by the values of the given columns
  def groupBy(first: String, rest: String*): GroupedDataStream = groupBy(first +: rest)
  def groupBy(fields: Iterable[String]): GroupedDataStream = groupBy(row => fields.map(row.get(_, false)).mkString("_"))

  // group by an arbitary function on the row data
  def groupBy(fn: Row => Any): GroupedDataStream = new GroupedDataStream {
    override def source: DataStream = outer
    override def keyFn: (Row) => Any = fn
    override def aggregations: Vector[Aggregation] = Vector.empty
  }

  def count: Long = CountAction(this).execute
  def size: Long = count

  /**
    * Action which results in all the rows being returned in memory as a Vector.
    * Alias for 'collect()'
    */
  def toVector: Vector[Row] = collect
  def toSet: Set[Row] = collect.toSet

  /**
    * Action which returns a scala.collection.CloseIterator, which will result in the
    * lazy evaluation of the stream, element by element.
    */
  def iterator: Iterator[Row] = IteratorAction(this).execute

  /**
    * Action which results in all the rows being returned in memory as a Vector.
    */
  def collect: Vector[Row] = VectorAction(this).execute

  def to(sink: Sink): Long = SinkAction(this, sink).execute()
  def to(sink: Sink, listener: Listener): Long = SinkAction(this, sink).execute(listener)

  def head: Row = partitions.foldLeft(None: Option[Row]) {
    (head, partition) => head orElse partition.iterator.take(1).toList.headOption
  }.get

  // -- actions --
  def fold[A](initial: A)(fn: (A, Row) => A): A = ??? // rows().foldLeft(initial)(fn)
  def forall(p: (Row) => Boolean): Boolean = ??? // ForallAction.execute(this, p)
  def exists(p: (Row) => Boolean): Boolean = ??? // ExistsAction.execute(this, p)
  def find(p: (Row) => Boolean): Option[Row] = ??? //  FindAction.execute(this, p)
}

object DataStream {


  import scala.reflect.runtime.universe._

  def fromIterator(_schema: StructType, rows: Iterator[Row]): DataStream = new DataStream {
    override def schema: StructType = _schema
    override private[eels] def partitions = Seq(CloseableIterator(rows))
  }

  /**
    * Create an in memory DataStream from the given Seq of Products.
    * The schema will be derived from the fields of the products using scala reflection.
    * This will result in a single partitioned DataStream.
    */
  def apply[T <: Product : TypeTag](ts: Seq[T]): DataStream = {
    val schema = StructType.from[T]
    val values = ts.map(_.productIterator.toVector)
    fromValues(schema, values)
  }

  def fromRows(_schema: StructType, first: Row, rest: Row*): DataStream = fromRows(_schema, first +: rest)

  def fromRows(_schema: StructType, rows: Seq[Row]): DataStream = new DataStream {
    override def schema: StructType = _schema
    override private[eels] def partitions = Seq(CloseableIterator(new Closeable {
      override def close(): Unit = ()
    }, rows.iterator))
  }

  /**
    * Create an in memory DataStream from the given Seq of values, and schema.
    * This will result in a single partitioned DataStream.
    */
  def fromValues(schema: StructType, values: Seq[Seq[Any]]): DataStream = {
    fromRows(schema, values.map(Row(schema, _)))
  }
}

case class VectorAction(ds: DataStream) {
  def execute: Vector[Row] = ds.coalesce.toIterable.toVector
}

case class SinkAction(ds: DataStream, sink: Sink) extends Logging {

  import com.sksamuel.exts.concurrent.ExecutorImplicits._

  def execute(listener: Listener = NoopListener): Long = {

    val schema = ds.schema
    val partitions = ds.partitions
    val total = new LongAdder
    val latch = new CountDownLatch(partitions.size)

    // each output stream will operate in an io thread.
    val io = Executors.newCachedThreadPool()

    // we open up a seperate output stream for each partition
    val streams = sink.open(schema, partitions.size)

    partitions.zip(streams).zipWithIndex.foreach { case ((CloseableIterator(closeable, iterator), stream), k) =>
      logger.debug(s"Starting writing task ${k + 1}")

      // each partition will have a buffer, which will be populated from a cpu thread.
      // then an io-bound thread will write from the buffer to the file/disk

      val buffer = new LinkedBlockingQueue[Row](100)
      ds.executor.execute(new Runnable {
        override def run(): Unit = {
          // each partition has its own stream, so once the iterator is finished we must notify the buffer
          try {
            iterator.foreach { row =>
              listener.onNext(row)
              buffer.put(row)
            }
            listener.onComplete()
          } catch {
            case NonFatal(e) =>
              logger.error("Error populating write buffer", e)
              listener.onError(e)
          } finally {
            logger.debug(s"Writing task ${k + 1} has completed")
            buffer.put(Row.Sentinel)
          }
        }
      })

      // the io-bound thread to copy from the buffer to the file
      io.submit {

        val localCount = new LongAdder

        try {
          BlockingQueueConcurrentIterator(buffer, Row.Sentinel).foreach { row =>
            stream.write(row)
            localCount.increment()
            total.increment()
          }
          logger.info(s"Partition ${k + 1} has completed; wrote ${localCount.sum} records; closing writer")
        } catch {
          case NonFatal(e) =>
            logger.info(s"Partition ${k + 1} has errored; wrote ${localCount.sum} records; closing writer", e)
        } finally {
          Try {
            closeable.close()
          }
          Try {
            stream.close()
          }
          latch.countDown()
        }
      }
    }

    io.shutdown()
    latch.await(21, TimeUnit.DAYS)
    total.sum()
  }
}