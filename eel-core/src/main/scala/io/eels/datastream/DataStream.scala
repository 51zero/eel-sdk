package io.eels.datastream

import java.io.Closeable
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicLong

import com.sksamuel.exts.Logging
import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import io.eels.schema.{DataType, Field, StringType, StructType}
import io.eels.{Flow, Listener, NoopListener, Row, Sink}

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

  /**
    * Returns the Schema for this stream. This call will not cause a full evaluation, but only
    * the operations required to retrieve a schema will occur. For example, on a stream backed
    * by a JDBC source, an empty resultset will be obtained in order to query the metadata for
    * the database columns.
    */
  def schema: StructType

  // the underlying flow for this data stream
  private[eels] def flows: Seq[Flow]

  // when this datastream is executed, causes all operations to this point to be parallelized
  def parallelize(n: Int, executor: Executor): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override private[eels] def flows = {
      val queue = Flow.toQueue(self.flows, executor)
      for (k <- 1 to n) yield {
        val iter = BlockingQueueConcurrentIterator(queue, Row.Sentinel)
        Flow(iter)
      }
    }
  }

  def map(f: Row => Row): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override private[eels] def flows = self.flows.map(_.map(f))
  }

  def filterNot(p: (Row) => Boolean): DataStream = filter { row => !p(row) }

  /**
    * For each row in the stream, filter drops any rows which do not match the predicate.
    */
  def filter(p: Row => Boolean): DataStream = new DataStream {
    override def schema: StructType = self.schema
    // we can keep each partition as is, and just filter individually
    override def flows: Seq[Flow] = {
      self.flows.map(_.filter(p))
    }
  }

  /**
    * Filters where the given field name matches the given predicate.
    */
  def filter(fieldName: String, p: (Any) => Boolean): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override def flows: Seq[Flow] = {
      val index = schema.indexOf(fieldName)
      if (index < 0)
        sys.error(s"Unknown field $fieldName")
      self.flows.map(_.filter { row => p(row.values(index)) })
    }
  }

  def projectionExpression(expr: String): DataStream = projection(expr.split(',').map(_.trim()))
  def projection(first: String, rest: String*): DataStream = projection((first +: rest).toList)

  /**
    * Returns a new DataStream which contains the given list of fields from the existing stream.
    */
  def projection(fields: Seq[String]): DataStream = new DataStream {
    override def schema: StructType = self.schema.projection(fields)
    override private[eels] def flows = {

      val oldSchema = self.schema
      val newSchema = schema

      self.flows.map { partition =>
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
    override def schema: StructType = self.schema
    override private[eels] def flows = {
      self.flows.map { partition =>
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
    override def schema: StructType = self.schema
    override private[eels] def flows = self.flows.map { partition =>
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
    override def schema: StructType = self.schema
    override private[eels] def flows = self.flows ++ other.flows
  }

  /**
    * Returns the same data but with an updated schema. The field that matches
    * the given name will have its datatype set to the given datatype.
    */
  def updateFieldType(fieldName: String, datatype: DataType): DataStream = new DataStream {
    override def schema: StructType = self.schema.updateFieldType(fieldName, datatype)
    override private[eels] def flows = {
      val updatedSchema = schema
      self.flows.map { part => part.map { row => Row(updatedSchema, row.values) } }
    }
  }

  def updateField(name: String, field: Field): DataStream = new DataStream {
    override def schema: StructType = self.schema.replaceField(name, field)
    override private[eels] def flows = {
      val updatedSchema = schema
      self.flows.map { partition =>
        partition.map { row => Row(updatedSchema, row.values) }
      }
    }
  }

  /**
    * Returns a new DataStream with the same data as this stream, but where the field names have been sanitized
    * by removing any occurances of the given characters.
    */
  def stripCharsFromFieldNames(chars: Seq[Char]): DataStream = new DataStream {
    override def schema: StructType = self.schema.stripFromFieldNames(chars)
    override private[eels] def flows = {
      val updatedschema = schema
      self.flows.map { partition =>
        partition.map { row => Row(updatedschema, row.values) }
      }
    }
  }

  /**
    * For each row, the value corresponding to the given fieldName is applied to the function.
    * The result of the function is the new value for that cell.
    */
  def replace(fieldName: String, fn: (Any) => Any): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override private[eels] def flows = {
      val index = schema.indexOf(fieldName)
      self.flows.map { partition =>
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
    override def schema: StructType = self.schema
    override private[eels] def flows = {
      val index = schema.indexOf(fieldName)
      self.flows.map { partition =>
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
    override def schema: StructType = self.schema
    override private[eels] def flows = self.flows.map { partition =>
      partition.flatMap { row => fn(row) }
    }
  }

  def replaceFieldType(from: DataType, toType: DataType): DataStream = new DataStream {
    override def schema: StructType = self.schema.replaceFieldType(from, toType)
    override private[eels] def flows = {
      val updatedSchema = schema
      self.flows.map { partition =>
        partition.map { row => Row(updatedSchema, row.values) }
      }
    }
  }

  /**
    * Foreach row, any values that match "from" will be replaced with "target".
    * This operation applies to all values for all rows.
    */
  def replace(from: String, target: Any): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override private[eels] def flows = {
      self.flows.map { partition =>
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
    val exists = self.schema.fieldNames().contains(field.name)
    if (exists) this else addField(field, defaultValue)
  }

  /**
    * Returns a new DataStream with the given field added at the end. The value of this field
    * for each Row is specified by the default value. The value must be compatible with the
    * field definition. Eg, an error will occur if the field has type Int and the default
    * value was 1.3
    */
  def addField(field: Field, defaultValue: Any): DataStream = new DataStream {
    override def schema: StructType = self.schema.addField(field)
    override def flows: Seq[Flow] = {
      val exists = self.schema.fieldNames().contains(field.name)
      if (exists) sys.error(s"Cannot add field ${field.name} as it already exists")
      val newSchema = schema
      self.flows.map { part => part.map(row => Row(newSchema, row.values :+ defaultValue)) }
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
    override def schema: StructType = self.schema
    override def flows: Seq[Flow] = self.flows.map(_.map { row =>
      fn(row)
      row
    })
  }

  def removeField(fieldName: String, caseSensitive: Boolean = true): DataStream = new DataStream {
    override def schema: StructType = self.schema.removeField(fieldName, caseSensitive)
    override private[eels] def flows = {
      val index = self.schema.indexOf(fieldName, caseSensitive)
      val newSchema = schema
      self.flows.map { partition =>
        partition.map { row =>
          val newValues = row.values.slice(0, index) ++ row.values.slice(index + 1, row.values.size)
          Row(newSchema, newValues)
        }
      }
    }
  }

  /**
    * Combines two datastreams together such that the fields from this datastream are joined with the fields
    * of the given datastream. Eg, if this datastream has A,B and the given datastream has C,D then the result will
    * be A,B,C,D
    *
    * This operation requires an executor, as it must buffer rows to ensure an even distribution.
    */
  def join(other: DataStream, executor: Executor): DataStream = new DataStream {

    override def schema: StructType = self.schema.join(other.schema)
    override private[eels] def flows = {

      val combinedSchema = schema

      val lq = Flow.toQueue(self.flows, executor)
      val rq = Flow.toQueue(other.flows, executor)

      val li = BlockingQueueConcurrentIterator(lq, Row.Sentinel)
      val ri = BlockingQueueConcurrentIterator(rq, Row.Sentinel)

      li.zip(ri).map { case (x, y) =>
        Row(combinedSchema, x.values ++ y.values)
      }

      Seq(Flow(iterator))
    }
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
    override def schema: StructType = self.schema.join(other.schema)
    override private[eels] def flows = {
      val joinedschema = schema
      val map = other.collect.map { row => row.get(key) -> row }.toMap
      self.flows.map { channel =>
        channel.map { row =>
          val value = row.get(key)
          val rhs = map(value)
          Row(joinedschema, row.values ++ rhs.values)
        }
      }
    }
  }

  def renameField(nameFrom: String, nameTo: String): DataStream = new DataStream {
    override def schema: StructType = self.schema.renameField(nameFrom, nameTo)
    override private[eels] def flows = {
      val updatedSchema = schema
      self.flows.map { CloseIterator =>
        CloseIterator.map { row => Row(updatedSchema, row.values) }
      }
    }
  }

  def takeWhile(fieldName: String, pred: Any => Boolean): DataStream = takeWhile(row => pred(row.get(fieldName)))
  def takeWhile(pred: Row => Boolean): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override def flows: Seq[Flow] = self.flows.map(_.takeWhile(pred))
  }

  /**
    * Returns a new DataStream where k number of rows only are processed.
    */
  def take(k: Long): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override def flows: Seq[Flow] = {
      val taken = new AtomicLong(0)
      self.flows.map { flow =>
        flow.takeWhile(_ => taken.incrementAndGet <= k)
      }
    }
  }

  /**
    * Returns a new DataStream where k number of rows has been dropped.
    */
  def drop(k: Long): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override def flows: Seq[Flow] = {
      val dropped = new AtomicLong(0)
      self.flows.map { flow =>
        flow.dropWhile(_ => dropped.incrementAndGet <= k)
      }
    }
  }

  def dropWhile(p: (Row) => Boolean): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override private[eels] def flows = self.flows.map(_.dropWhile(p))
  }

  def dropWhile(fieldName: String, pred: (Any) => Boolean): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override private[eels] def flows = {
      val index = self.schema.indexOf(fieldName)
      self.flows.map(_.dropWhile { row => pred(row.values(index)) })
    }
  }

  // returns a new DataStream with any rows that contain one or more nulls excluded
  def dropNullRows(): DataStream = new DataStream {
    override def schema: StructType = self.schema
    override private[eels] def flows = self.flows.map { partition => partition.filterNot(_.values.contains(null)) }
  }

  def withLowerCaseSchema(): DataStream = new DataStream {
    private lazy val lowerSchema = self.schema.toLowerCase()
    override def schema: StructType = lowerSchema
    override def flows: Seq[Flow] = self.flows
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

  def to(sink: Sink): Long = to(sink, NoopListener)
  def to(sink: Sink, listener: Listener): Long = SinkAction(this, sink).execute(listener)

  def head: Row = flows.foldLeft(None: Option[Row]) {
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
    override private[eels] def flows = Seq(Flow(rows))
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
    override private[eels] def flows = Seq(Flow(rows.iterator))
  }

  /**
    * Create an in memory DataStream from the given Seq of values, and schema.
    * This will result in a single partitioned DataStream.
    */
  def fromValues(schema: StructType, values: Seq[Seq[Any]]): DataStream = {
    fromRows(schema, values.map(Row(schema, _)))
  }
}