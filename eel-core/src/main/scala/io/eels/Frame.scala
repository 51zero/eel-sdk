package io.eels

import io.eels.actions._
import io.eels.schema._

import scala.language.implicitConversions

trait Frame {
  outer =>

  def schema: StructType
  def rows(): CloseableIterator[Row]

  /**
    * Combines two frames together such that the fields from this frame are joined with the fields
    * of the given frame. Eg, if this frame has A,B and the given frame has C,D then the result will
    * be A,B,C,D
    */
  def join(other: Frame): Frame = new Frame {
    override def schema: StructType = outer.schema.join(other.schema)
    override def rows(): CloseableIterator[Row] = {
      val combinedSchema = schema
      outer.rows.zip(other.rows).map { case (t, u) =>
        Row(combinedSchema, t.values ++ u.values)
      }
    }
  }

  /**
    * Foreach row, any values that match "from" will be replaced with "target".
    * This operation applies to all values for all rows.
    */
  def replace(from: String, target: Any): Frame = new Frame {
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = {
      outer.rows.map { row =>
        val values = row.values.map { value =>
          if (value == from) target else value
        }
        Row(row.schema, values)
      }
    }
  }

  /**
    * Replaces any values that match "form" with the value "target".
    * This operation only applies to the field name specified.
    */
  def replace(fieldName: String, from: String, target: Any): Frame = new Frame {
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = {
      val index = schema.indexOf(fieldName)
      outer.rows.map { row =>
        val existing = row.values(index)
        if (existing == from) {
          Row(row.schema, row.values.updated(index, target))
        } else {
          row
        }
      }
    }
  }

  /**
    * For each row, the value corresponding to the given fieldName is applied to the function.
    * The result of the function is the new value for that cell.
    */
  def replace(fieldName: String, fn: (Any) => Any): Frame = new Frame {
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = {
      val index = schema.indexOf(fieldName)
      outer.rows.map { row =>
        val newValues = row.values.updated(index, fn(row.values(index)))
        Row(row.schema, newValues)
      }
    }
  }

  def listener(listener: Listener): Frame = foreach(listener.onNext)

  def take(n: Int) = new Frame {
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = outer.rows().take(n)
  }

  def takeWhile(pred: (Row) => Boolean): Frame = new Frame {
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = outer.rows.takeWhile(pred)
  }

  def takeWhile(fieldName: String, pred: (Any) => Boolean): Frame = new Frame {
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = {
      val index = outer.schema.indexOf(fieldName)
      outer.rows().takeWhile { row => pred(row.values(index)) }
    }
  }

  def updateFieldType(fieldName: String, fieldType: DataType): Frame = new Frame {
    override def schema: StructType = outer.schema.updateFieldType(fieldName, fieldType)
    override def rows(): CloseableIterator[Row] = outer.rows()
  }

  def dropWhile(pred: (Row) => Boolean): Frame = new Frame {
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = outer.rows().dropWhile(pred)
  }

  def dropWhile(fieldName: String, pred: (Any) => Boolean): Frame = new Frame {
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = {
      val index = outer.schema.indexOf(fieldName)
      outer.rows().dropWhile { row => pred(row.values(index)) }
    }
  }

  /**
    * Returns a new Frame where only each "k" row is retained. Ie, if sample is 2, then on average,
    * every other row will be returned. If sample is 10 then only 10% of rows will be returned.
    * When running concurrently, the rows that are sampled will vary depending on the ordering that the
    * workers pull through the rows. Each stream (thread) uses its own count for the sample.
    */
  def sample(k: Int): Frame = new Frame {
    override def schema: StructType = outer.schema
    // todo add impl
    override def rows(): CloseableIterator[Row] = outer.rows()
  }

  /**
    * Returns a new Frame with the new field of type String added at the end. The value of
    * this field for each Row is specified by the default value.
    */
  def addField(name: String, defaultValue: Any): Frame = addField(Field(name, StringType), defaultValue)

  /**
    * Returns a new Frame with the given field added at the end. The value of this field
    * for each Row is specified by the default value. The value must be compatible with the
    * field definition. Eg, an error will occur if the field had type Int and the default
    * value was 1.3
    */
  def addField(field: Field, defaultValue: Any): Frame = new Frame {
    override def schema: StructType = outer.schema.addField(field)
    override def rows(): CloseableIterator[Row] = {
      val exists = outer.schema.fieldNames().contains(field.name)
      if (exists) sys.error(s"Cannot add field ${field.name} as it already exists")
      val newSchema = schema
      outer.rows().map { row => Row(newSchema, row.values :+ defaultValue) }
    }
  }

  def replaceFieldType(from: DataType, toType: DataType): Frame = new Frame {
    override def schema: StructType = outer.schema.replaceFieldType(from, toType)
    override def rows(): CloseableIterator[Row] = {
      val newSchema = schema
      outer.rows().map { row => Row(newSchema, row.values) }
    }
  }

  // allows aggregations on the entire dataset
  def aggregated(): GroupedFrame = new GroupedFrame {
    override def source: Frame = outer
    override def keyFn: Row => Any = GroupedFrame.FullDatasetKeyFn
    override def aggregations: Vector[Aggregation] = Vector.empty
  }

  // group by the values of the given columns
  def groupBy(first: String, rest: String*): GroupedFrame = groupBy(first +: rest)
  def groupBy(fields: Iterable[String]): GroupedFrame = groupBy(row => fields.map(row.get(_, false)).mkString("_"))

  // group by an arbitary function on the row data
  def groupBy(fn: Row => Any): GroupedFrame = new GroupedFrame {
    override def source: Frame = outer
    override def keyFn = fn
    override def aggregations: Vector[Aggregation] = Vector.empty
  }

  def addFieldIfNotExists(name: String, defaultValue: Any): Frame = addFieldIfNotExists(Field(name, StringType), defaultValue)

  def addFieldIfNotExists(field: Field, defaultValue: Any): Frame = {
    val exists = outer.schema.fieldNames().contains(field.name)
    if (exists) this else addField(field, defaultValue)
  }

  def removeField(fieldName: String, caseSensitive: Boolean = true): Frame = new Frame {
    override def schema: StructType = outer.schema.removeField(fieldName, caseSensitive)
    override def rows(): CloseableIterator[Row] = {
      val index = outer.schema.indexOf(fieldName, caseSensitive)
      val newSchema = schema
      outer.rows().map { row =>
        val newValues = row.values.slice(0, index) ++ row.values.slice(index + 1, row.values.size)
        Row(newSchema, newValues)
      }
    }
  }

  def updateField(field: Field): Frame = new Frame {
    override def schema: StructType = outer.schema.replaceField(field.name, field)
    override def rows(): CloseableIterator[Row] = {
      throw new UnsupportedOperationException()
    }
  }

  def renameField(nameFrom: String, nameTo: String): Frame = new Frame {
    override def schema: StructType = outer.schema.renameField(nameFrom, nameTo)
    override def rows(): CloseableIterator[Row] = outer.rows()
  }

  /**
    * Returns a new Frame with the same data as this frame, but where the field names have been sanitized
    * by removing any occurances of the given characters.
    */
  def stripCharsFromFieldNames(chars: Seq[Char]): Frame = new Frame {
    override def schema: StructType = outer.schema.stripFromFieldNames(chars)
    override def rows(): CloseableIterator[Row] = outer.rows()
  }

  def explode(fn: (Row) => Seq[Row]): Frame = new Frame {
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = outer.rows().flatMap(fn)
  }

  def replaceNullValues(defaultValue: String): Frame = new Frame {
    override def schema: StructType = outer.schema
    override def rows() = outer.rows().map { row =>
      val newValues = row.values.map {
        case null => defaultValue
        case otherwise => otherwise
      }
      Row(row.schema, newValues)
    }
  }

  /**
    * Joins two frames together, such that the elements of the given frame are appended to the
    * end of this frame. This operation is the same as a concat operation.
    */
  def ++(other: Frame): Frame = union(other)
  def union(other: Frame): Frame = new Frame {
    // todo check schemas are compatible
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = outer.rows() concat other.rows()
  }

  def projectionExpression(expr: String): Frame = projection(expr.split(',').map(_.trim()))
  def projection(first: String, rest: String*): Frame = projection((first +: rest).toList)

  /**
    * Returns a new frame which contains the given list of fields from the existing frame.
    */
  def projection(fields: Seq[String]): Frame = new Frame {

    override def schema: StructType = outer.schema.projection(fields)

    override def rows(): CloseableIterator[Row] = {

      val oldSchema = outer.schema
      val newSchema = schema

      outer.rows().map { row =>
        val values = newSchema.fieldNames().map { name =>
          val k = oldSchema.indexOf(name)
          row.values(k)
        }
        Row(newSchema, values)
      }
    }
  }

  /**
    * Execute a side effecting function for every row in the frame, returning the same row.
    *
    * @param fn the function to execute
    * @return this frame, to allow for builder style chaining
    */
  def foreach[U](fn: (Row) => U): Frame = new Frame {
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = outer.rows().map { row =>
      fn(row)
      row
    }
  }

  /**
    * Returns a new Frame where the schema has been lowercased.
    * This does not affect values.
    */
  @deprecated("use datastream", "1.2.0")
  def withLowerCaseSchema(): Frame = new Frame {
    private lazy val lowerSchema = outer.schema.toLowerCase()
    override def schema: StructType = lowerSchema
    override def rows(): CloseableIterator[Row] = outer.rows().map { row =>
      row.replaceSchema(lowerSchema)
    }
  }

  @deprecated("use datastream", "1.2.0")
  def drop(k: Int): Frame = new Frame {
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = outer.rows().drop(k)
  }

  @deprecated("use datastream", "1.2.0")
  def map(f: (Row) => Row): Frame = new Frame {
    override def rows(): CloseableIterator[Row] = outer.rows().map(f)
    override def schema: StructType = outer.schema
  }

  def filterNot(p: (Row) => Boolean): Frame = filter { str => !p(str) }

  @deprecated("use datastream", "1.2.0")
  def filter(p: (Row) => Boolean): Frame = new Frame {
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = outer.rows().filter(p)
  }

  /**
    * Filters where the given field name matches the given predicate.
    */
  def filter(fieldName: String, p: (Any) => Boolean): Frame = new Frame {
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = {
      val index = schema.indexOf(fieldName)
      if (index < 0)
        sys.error(s"Unknown field $fieldName")
      outer.rows().filter { row =>
        p(row.values(index))
      }
    }
  }

  // returns a new Frame with any rows that contain one or more nulls excluded
  def dropNullRows(): Frame = new Frame {
    override def schema: StructType = outer.schema
    override def rows(): CloseableIterator[Row] = outer.rows().filter { row =>
      !row.values.contains(null)
    }
  }

  // -- actions --
  def fold[A](initial: A)(fn: (A, Row) => A): A = rows().foldLeft(initial)(fn)
  def forall(p: (Row) => Boolean): Boolean = ForallAction.execute(this, p)
  def exists(p: (Row) => Boolean): Boolean = ExistsAction.execute(this, p)
  def find(p: (Row) => Boolean): Option[Row] = FindAction.execute(this, p)

  def head(): Row = rows().head

  // alias for to
  def save(sink: Sink, listener: Listener = NoopListener): Long = to(sink, listener)
  def to(sink: Sink, listener: Listener = NoopListener): Long = SinkAction.execute(sink, this, listener)
  def size(): Long = CountAction.execute(this)

  // alias for size()
  def count(): Long = size()

  // alias for toVector()
  def collect(): Vector[Row] = rows().toVector

  def toSeq(): Seq[Row] = collect
  @deprecated("use toVector")
  def toList(): List[Row] = rows().toList
  def toVector(): Vector[Row] = collect
  def toSet(): Set[Row] = toVector().toSet
}

object Frame {

  import scala.reflect.runtime.universe._

  def apply[T <: Product : TypeTag](ts: Seq[T]): Frame = {
    val schema = StructType.from[T]
    val rows = ts.map { t => Row(schema, t.productIterator.toVector) }
    Frame(schema, rows)
  }

  def fromValues(schema: StructType, _rows: Seq[Seq[Any]]): Frame = {
    apply(schema, _rows.map(values => Row(schema, values)))
  }

  def fromValues(schema: StructType, first: Seq[Any], rest: Seq[Any]*): Frame = fromValues(schema, first +: rest)

  def apply(_schema: StructType, first: Row, rest: Row*): Frame = apply(_schema, first +: rest)

  def apply(_schema: StructType, _rows: Seq[Row]): Frame = new Frame {
    override def schema: StructType = _schema
    override def rows(): CloseableIterator[Row] = CloseableIterator.fromIterable(_rows)
  }

  def fromIterator(_schema: StructType, _iter: Iterator[Row]): Frame = new Frame {
    override def schema: StructType = _schema
    override def rows() = new CloseableIterator[Row] {
      override val iterator: Iterator[Row] = _iter
    }
  }
}
