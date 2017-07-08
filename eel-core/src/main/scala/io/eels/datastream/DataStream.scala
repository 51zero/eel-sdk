package io.eels.datastream

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{CountDownLatch, Executor}

import com.sksamuel.exts.Logging
import io.eels.schema.{DataType, Field, StringType, StructType}
import io.eels.{DataTable, Listener, Record, Row, Sink}

import scala.collection.immutable.VectorIterator
import scala.language.implicitConversions

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
  * A DataStream is split into one or more flows. Each flow can operate independantly
  * of the others. For example, if you filter a flow, each flow will be filtered seperately,
  * which allows it to be parallelized. If you write out a flow, each partition can be written out
  * to individual files, again allowing parallelization.
  *
  */
trait DataStream extends Logging {
  self =>

  def schema: StructType

  def subscribe(subscriber: Subscriber[Seq[Row]]): Unit

  def map(f: Row => Row): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = subscriber.next(t.map(f))
      })
    }
  }

  def filter(f: Row => Boolean): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = subscriber.next(t.filter(f))
      })
    }
  }

  /**
    * Filters where the given field name matches the given predicate.
    */
  def filter(fieldName: String, p: (Any) => Boolean): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        val index = schema.indexOf(fieldName)
        if (index < 0)
          sys.error(s"Unknown field $fieldName")
        override def next(t: Seq[Row]): Unit = {
          subscriber next t.filter { row => p(row.values(index)) }
        }
      })
    }
  }

  def withLowerCaseSchema(): DataStream = new DataStream {
    override def schema: StructType = self.schema.toLowerCase()
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        val lower = schema
        override def next(t: Seq[Row]): Unit = {
          val ts = t.map { row => Row(lower, row.values) }
          subscriber.next(ts)
        }
      })
    }
  }

  def filterNot(p: (Row) => Boolean): DataStream = filter { row => !p(row) }

  def take(n: Int): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      val count = new AtomicInteger(0)
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = {
          val remaining = n - count.get
          if (remaining > 0) {
            val ts = t.take(remaining)
            count.addAndGet(ts.size)
            subscriber.next(ts)
          }
        }
      })
    }
  }

  def drop(n: Int): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      var left = n
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = {
          val todrop = Math.min(left, t.size)
          left = left - todrop
          subscriber next t.drop(todrop)
        }
      })
    }
  }

  // allows aggregations on the entire dataset
  def aggregated(): GroupedDataStream = new GroupedDataStream {
    override def source: DataStream = self
    override def keyFn: Row => Any = GroupedDataStream.FullDatasetKeyFn
    override def aggregations: Vector[Aggregation] = Vector.empty
  }

  // group by the values of the given columns
  def groupBy(first: String, rest: String*): GroupedDataStream = groupBy(first +: rest)
  def groupBy(fields: Iterable[String]): GroupedDataStream = groupBy(row => fields.map(row.get(_, false)).mkString("_"))

  // group by an arbitary function on the row data
  def groupBy(fn: Row => Any): GroupedDataStream = new GroupedDataStream {
    override def source: DataStream = self
    override def keyFn: (Row) => Any = fn
    override def aggregations: Vector[Aggregation] = Vector.empty
  }

  def iterator: VectorIterator[Row] = collect.iterator

  def listener(_listener: Listener): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      self.subscribe(new Subscriber[Seq[Row]] {
        override def next(t: Seq[Row]): Unit = {
          subscriber.next(t)
          try {
            t.foreach(_listener.onNext)
          } catch {
            case t: Throwable =>
              subscriber.error(t)
              _listener.onError(t)
            // todo need to then cancel the subscription
          }
        }
        override def started(s: Cancellable): Unit = {
          subscriber.started(s)
          try {
            _listener.started()
          } catch {
            case t: Throwable =>
              subscriber.error(t)
              _listener.onError(t)
            // todo need to then cancel the subscription
          }
        }
        override def completed(): Unit = {
          subscriber.completed()
          _listener.onComplete()
        }
        override def error(t: Throwable): Unit = {
          subscriber.error(t)
          _listener.onError(t)
        }
      })
    }
  }

  /**
    * Returns the same data but with an updated schema. The field that matches
    * the given name will have its datatype set to the given datatype.
    */
  def updateFieldType(fieldName: String, datatype: DataType): DataStream = new DataStream {
    override def schema: StructType = self.schema.updateFieldType(fieldName, datatype)
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      val updatedSchema = schema
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = {
          subscriber next t.map(row => Row(updatedSchema, row.values))
        }
      })
    }
  }

  def updateField(name: String, field: Field): DataStream = new DataStream {
    override def schema: StructType = self.schema.replaceField(name, field)
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      val updatedSchema = schema
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = {
          subscriber next t.map(row => Row(updatedSchema, row.values))
        }
      })
    }
  }

  /**
    * Execute a side effecting function for every row in the stream, returning the same row.
    */
  def foreach[U](fn: (Row) => U): DataStream = map { row => fn(row); row }

  /**
    * Combines two datastreams together such that the fields from this datastream are joined with the fields
    * of the given datastream. Eg, if this datastream has A,B and the given datastream has C,D then the result will
    * be A,B,C,D
    *
    * This operation requires an executor, as it must buffer rows to ensure an even distribution.
    */
  def join(other: DataStream, executor: Executor): DataStream = new DataStream {
    override def schema: StructType = ???
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = ???
  }

  /**
    * Joins the given datastream to this datastream on the given key column,
    * where the values of the keys are equal as taken by the scala == operator.
    * Both datastreams must contain the key column.
    *
    * The given datastream is fully inflated when this datastream needs to be materialized.
    * For that reason, always use the smallest datastream as the parameter, and the larger
    * datastream as the receiver.
    */
  def join(key: String, other: DataStream): DataStream = new DataStream {
    override def schema: StructType = ???
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = ???
  }

  def renameField(nameFrom: String, nameTo: String): DataStream = new DataStream {
    override def schema: StructType = self.schema.renameField(nameFrom, nameTo)
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      val updatedSchema = schema
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit =
          subscriber next t.map { row => Row(updatedSchema, row.values) }
      })
    }
  }

  // returns a new DataStream with any rows that contain one or more nulls excluded
  def dropNullRows(): DataStream = filterNot(_.values.contains(null))

  def removeField(fieldName: String, caseSensitive: Boolean = true): DataStream = new DataStream {
    override def schema: StructType = self.schema.removeField(fieldName, caseSensitive)
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      val index = self.schema.indexOf(fieldName, caseSensitive)
      val newSchema = schema
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = {
          subscriber next t.map { row =>
            val newValues = row.values.slice(0, index) ++ row.values.slice(index + 1, row.values.size)
            Row(newSchema, newValues)
          }
        }
      })
    }
  }

  /**
    * Returns a new DataStream with the same data as this stream, but where the field names have been sanitized
    * by removing any occurances of the given characters.
    */
  def stripCharsFromFieldNames(chars: Seq[Char]): DataStream = new DataStream {
    override def schema: StructType = self.schema.stripFromFieldNames(chars)
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      val updatedSchema = schema
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit =
          subscriber next t.map { row => Row(updatedSchema, row.values) }
      })
    }
  }

  /**
    * For each row, the value corresponding to the given fieldName is applied to the function.
    * The result of the function is the new value for that cell.
    */
  def replace(fieldName: String, fn: (Any) => Any): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      val index = schema.indexOf(fieldName)
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = {
          subscriber next t.map { row =>
            val newValues = row.values.updated(index, fn(row.values(index)))
            Row(row.schema, newValues)
          }
        }
      })
    }
  }

  /**
    * Replaces any values that match "form" with the value "target".
    * This operation only applies to the field name specified.
    */
  def replace(fieldName: String, from: String, target: Any): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      val index = schema.indexOf(fieldName)
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = {
          subscriber next t.map { row =>
            val existing = row.values(index)
            if (existing == from) {
              Row(row.schema, row.values.updated(index, target))
            } else {
              row
            }
          }
        }
      })
    }
  }

  /**
    * Foreach row, any values that match "from" will be replaced with "target".
    * This operation applies to all values for all rows.
    */
  def replace(from: String, target: Any): DataStream = map { row =>
    val values = row.values.map { value =>
      if (value == from) target else value
    }
    Row(row.schema, values)
  }

  /**
    * Returns a new DataStream where only each "k" row is retained. Ie, if sample is 2, then on average,
    * every other row will be returned. If sample is 10 then only 10% of rows will be returned.
    * When running concurrently, the rows that are sampled will vary depending on the ordering that the
    * workers pull through the rows. Each partition uses its own couter.
    */
  def sample(k: Int): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      val counter = new AtomicLong(0)
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = {
          subscriber next t.filter { row =>
            if (counter.getAndIncrement % k == 0) false
            else true
          }
        }
      })
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
    override def schema: StructType = self.schema
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = ???
  }

  def projectionExpression(expr: String): DataStream = projection(expr.split(',').map(_.trim()))
  def projection(first: String, rest: String*): DataStream = projection((first +: rest).toList)

  /**
    * Returns a new DataStream which contains the given list of fields from the existing stream.
    */
  def projection(fields: Seq[String]): DataStream = new DataStream {

    override def schema: StructType = self.schema.projection(fields)

    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {

      val oldSchema = self.schema
      val newSchema = schema

      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = {
          val ts = t.map { row =>
            val values = newSchema.fieldNames().map { name =>
              val k = oldSchema.indexOf(name)
              row.values(k)
            }
            Row(newSchema, values)
          }
          subscriber.next(ts)
        }
      })
    }
  }

  def replaceNullValues(defaultValue: String): DataStream = new DataStream {
    override def schema: StructType = self.schema

    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = {
          val ts = t.map { row =>
            val newValues = row.values.map {
              case null => defaultValue
              case otherwise => otherwise
            }
            Row(row.schema, newValues)
          }
          subscriber.next(ts)
        }
      })
    }
  }

  def addFieldIfNotExists(name: String, defaultValue: Any): DataStream =
    addFieldIfNotExists(Field(name, StringType), defaultValue)

  def addFieldIfNotExists(field: Field, defaultValue: Any): DataStream = {
    val exists = self.schema.fieldNames().contains(field.name)
    if (exists) this else addField(field, defaultValue)
  }

  /**
    * Returns a new DataStream with the new field of type String added at the end. The value of
    * this field for each Row is specified by the default value.
    */
  def addField(name: String, defaultValue: String): DataStream = addField(Field(name, StringType), defaultValue)

  /**
    * Returns a new DataStream with the given field added at the end. The value of this field
    * for each Row is specified by the default value. The value must be compatible with the
    * field definition. Eg, an error will occur if the field has type Int and the default
    * value was 1.3
    */
  def addField(field: Field, defaultValue: Any): DataStream = new DataStream {
    override def schema: StructType = self.schema.addField(field)
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      val exists = self.schema.fieldNames().contains(field.name)
      if (exists) sys.error(s"Cannot add field ${field.name} as it already exists")
      val newSchema = schema
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = subscriber.next(t.map(row => Row(newSchema, row.values :+ defaultValue)))
      })
    }
  }

  /**
    * Action which results in all the rows being returned in memory as a Vector.
    */
  def collect: Vector[Row] = {
    val vector = Vector.newBuilder[Row]
    subscribe(new Subscriber[Seq[Row]] {
      override def next(t: Seq[Row]): Unit = t.foreach(vector.+=)
      override def started(subscription: Cancellable): Unit = ()
      override def completed(): Unit = ()
      override def error(t: Throwable): Unit = ()
    })
    vector.result()
  }

  def explode(fn: (Row) => Seq[Row]): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = {
          subscriber.next(t.flatMap(fn))
        }
      })
    }
  }

  def replaceFieldType(from: DataType, toType: DataType): DataStream = new DataStream {
    override def schema: StructType = self.schema.replaceFieldType(from, toType)
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      val updatedSchema = schema
      self.subscribe(new DelegateSubscriber[Seq[Row]](subscriber) {
        override def next(t: Seq[Row]): Unit = {
          subscriber next t.map { row => Row(updatedSchema, row.values) }
        }
      })
    }
  }

  def count: Long = size
  def size: Long = {
    var count = 0L
    val latch = new CountDownLatch(1)
    subscribe(new Subscriber[Seq[Row]] {
      override def next(t: Seq[Row]): Unit = count = count + t.size
      override def started(subscription: Cancellable): Unit = ()
      override def completed(): Unit = latch.countDown()
      override def error(t: Throwable): Unit = ()
    })
    latch.await()
    count
  }

  def head: Row = collect.head

  // -- actions --
  def fold[A](initial: A)(fn: (A, Row) => A): A = ??? // rows().foldLeft(initial)(fn)
  def forall(p: (Row) => Boolean): Boolean = ??? // ForallAction.execute(this, p)
  def exists(p: (Row) => Boolean): Boolean = ??? // ExistsAction.execute(this, p)
  def find(p: (Row) => Boolean): Option[Row] = ??? //  FindAction.execute(this, p)

  def to(sink: Sink): Long = to(sink, 1)
  def to(sink: Sink, parallelism: Int): Long = SinkAction(this, sink, parallelism).execute()

  /**
    * Action which results in all the rows being returned in memory as a Vector.
    * Alias for 'collect()'
    */
  def toVector: Vector[Row] = collect
  def toSet: Set[Row] = collect.toSet

  def toDataTable: DataTable = DataTable(schema, collect.map(_.values).map(Record.apply))
}

object DataStream {

  import scala.reflect.runtime.universe._

  def fromIterator(_schema: StructType, rows: Iterator[Row]): DataStream = new DataStream {
    override def schema: StructType = _schema
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      try {
        rows.grouped(1000).foreach(subscriber.next)
        subscriber.completed()
      } catch {
        case t: Throwable => subscriber.error(t)
      }
    }
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
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      try {
        rows.grouped(1000).foreach(subscriber.next)
        subscriber.completed()
      } catch {
        case t: Throwable => subscriber.error(t)
      }
    }
  }

  /**
    * Create an in memory DataStream from the given Seq of values, and schema.
    * This will result in a single partitioned DataStream.
    */
  def fromValues(schema: StructType, values: Seq[Seq[Any]]): DataStream = {
    fromRows(schema, values.map(Row(schema, _)))
  }
}