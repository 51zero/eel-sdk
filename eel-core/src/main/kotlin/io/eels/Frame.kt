package io.eels

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import org.apache.hadoop.hdfs.server.namenode.Content
import rx.Observable

interface Frame {

  val defaultBufferSize: Int
    get() = 1000

  fun outer() = this

  fun schema(): Schema

  fun observable(): Observable<Row>

  /**
   * Combines two frames together such that the columns from this frame are joined with the columns
   * of the given frame. Eg, if this frame has A,B and the given frame has C,D then the result will
   * be A,B,C,D
   */
  fun join(other: Frame): Frame = object : Frame {
    override fun schema(): Schema = outer().schema().join(other.schema())

    override fun observable(): Observable<Row> {
      val schema = schema()
      val func: (Row, Row) -> Row = { a, b -> Row(schema, a.values.plus(b.values)) }
      return outer().observable().zipWith(other.observable(), func)
    }
  }

  /**
   * Foreach row, any values that match "from" will be replaced with "target".
   * This operation applies to all values for all rows.
   */
  fun replace(from: String, target: Any?): Frame = object : Frame {
    override fun schema(): Schema = outer().schema()
    override fun observable(): Observable<Row> = outer().observable().map {
      val values = it.values.map { if (it == from) target else it }
      Row(it.schema, values)
    }
  }

  /**
   * Replaces any values that match "form" with the value "target".
   * This operation only applies to the column name specified.
   */
  fun replace(columnName: String, from: String, target: Any?): Frame = object : Frame {
    override fun schema(): Schema = outer().schema()
    override fun observable(): Observable<Row> {
      val index = schema().indexOf(columnName)
      return outer().observable().map {
        val values = it.values.toMutableList()
        if (values[index] == from)
          values[index] = target
        Row(it.schema, values.toList())
      }
    }
  }

  /**
   * For each row, the value corresponding to the given columnName is applied to the function.
   * The result of the function is the new value for that cell.
   */
  fun replace(columnName: String, fn: (Any?) -> Any?): Frame = object : Frame {
    override fun schema(): Schema = outer().schema()
    override fun observable(): Observable<Row> {
      val index = schema().indexOf(columnName)
      return outer().observable().map {
        val values = it.values.toMutableList()
        values[index] = fn(values[index])
        Row(it.schema, values.toList())
      }
    }
  }

  fun takeWhile(pred: (Row) -> Boolean): Frame = object : Frame {
    override fun schema(): Schema = outer().schema()
    override fun observable(): Observable<Row> = outer().observable().takeWhile(pred)
  }

  fun takeWhile(columnName: String, pred: (Any?) -> Boolean): Frame = object : Frame {
    override fun schema(): Schema = outer().schema()
    override fun observable(): Observable<Row> {
      val index = outer().schema().indexOf(columnName)
      return outer().observable().takeWhile { pred(it.values[index]) }
    }
  }

  fun updateColumnType(columnName: String, fieldType: FieldType): Frame = object : Frame {
    override fun schema(): Schema = outer().schema().updateColumnType(columnName, fieldType)
    override fun observable(): Observable<Row> = outer().observable()
  }

  fun dropWhile(pred: (Row) -> Boolean): Frame = object : Frame {
    override fun schema(): Schema = outer().schema()
    override fun observable(): Observable<Row> = outer().observable().skipWhile(pred)
  }

  fun dropWhile(columnName: String, pred: (Any?) -> Boolean): Frame = object : Frame {
    override fun schema(): Schema = outer().schema()
    override fun observable(): Observable<Row> {
      val index = outer().schema().indexOf(columnName)
      return outer().observable().skipWhile { pred(it.values[index]) }
    }
  }

  /**
   * Returns a new Frame where only each "k" row is retained. Ie, if sample is 2, then on average,
   * every other row will be returned. If sample is 10 then only 10% of rows will be returned.
   * When running concurrently, the rows that are sampled will vary depending on the ordering that the
   * workers pull through the rows. Each stream (thread) uses its own count for the sample.
   */
  fun sample(k: Int): Frame = object : Frame {
    override fun schema(): Schema = outer().schema()
    override fun observable(): Observable<Row> = outer().observable()
  }

  /**
   * Returns a new Frame with the new column of type String added at the end. The value of
   * this column for each Row is specified by the default value.
   */
  fun addColumn(name: String, defaultValue: String): Frame = addColumn(Field(name), defaultValue)

  /**
   * Returns a new Frame with the given column added at the end. The value of this column
   * for each Row is specified by the default value. The value must be compatible with the
   * Column definition. Eg, an error will occur if the Column had type Int and the default
   * value was 1.3
   */
  fun addColumn(field: Field, defaultValue: Any): Frame = object : Frame {
    override fun schema(): Schema = outer().schema().addColumn(field)
    override fun observable(): Observable<Row> {
      val exists = outer().schema().columnNames().contains(field.name)
      if (exists) throw IllegalArgumentException("Cannot add column $field as it already exists")
      val newSchema = schema()
      return outer().observable().map { Row(newSchema, it.values.plus(defaultValue)) }
    }
  }

  fun addColumnIfNotExists(name: String, defaultValue: Any): Frame = addColumnIfNotExists(Field(name), defaultValue)

  fun addColumnIfNotExists(field: Field, defaultValue: Any): Frame = object : Frame {
    override fun schema(): Schema = outer().schema().addColumnIfNotExists(field)
    override fun observable(): Observable<Row> {
      val exists = outer().schema().columnNames().contains(field.name)
      return if (exists) outer().observable() else addColumn(field, defaultValue).observable()
    }
  }

  fun removeColumn(columnName: String, caseSensitive: Boolean = true): Frame = object : Frame {
    override fun schema(): Schema = outer().schema().removeColumn(columnName, caseSensitive)
    override fun observable(): Observable<Row> {
      val index = outer().schema().indexOf(columnName, caseSensitive)
      val newSchema = schema()
      return outer().observable().map {
        val newValues = it.values.slice(0..index).plus(it.values.slice(index + 1..it.values.size))
        Row(newSchema, newValues)
      }
    }
  }

  fun updateColumn(field: Field): Frame = object : Frame {
    override fun observable(): Observable<Row> {
      throw UnsupportedOperationException()
    }

    override fun schema(): Schema = outer().schema().updateColumn(field)

    //    override fun buffer(): Buffer = object : Buffer {
    //      val buffer = outer().buffer()
    //      val index = outer().schema().indexOf(column)
    //      override fun close(): Unit = buffer.close()
    //      override fun stream(): Observable<Row> = buffer.stream()
    //    }
  }

  fun renameColumn(nameFrom: String, nameTo: String): Frame = object : Frame {
    override fun schema(): Schema = outer().schema().renameColumn(nameFrom, nameTo)
    override fun observable(): Observable<Row> = outer().observable()
  }

  fun stripFromColumnName(chars: List<Char>): Frame = object : Frame {
    override fun schema(): Schema = outer().schema().stripFromColumnName(chars)
    override fun observable(): Observable<Row> = outer().observable()
  }

  fun explode(fn: (Row) -> List<Row>): Frame = object : Frame {
    override fun observable(): Observable<Row> = outer().observable().flatMap { Observable.from(fn(it)) }
    override fun schema(): Schema = outer().schema()
  }

  fun fill(defaultValue: String): Frame = object : Frame {
    override fun observable(): Observable<Row> = observable().map {
      val newValues = it.values.map {
        when (it) {
          null -> defaultValue
          else -> it
        }
      }
      Row(it.schema, newValues)
    }

    override fun schema(): Schema = outer().schema()
  }

  /**
   * Joins two frames together, such that the elements of the given frame are appended to the
   * end of this frame. This operation is the same as a concat operation.
   */
  fun union(other: Frame): Frame = object : Frame {
    // todo check schemas are compatible
    override fun schema(): Schema = outer().schema()

    override fun observable(): Observable<Row> = outer().observable().concatWith(other.observable())
  }

  fun projectionExpression(expr: String): Frame = projection(expr.split(',').map { it.trim() })
  fun projection(vararg columns: String): Frame = projection(columns.asList())

  /**
   * Returns a new frame which contains the given list of columns from the existing frame.
   */
  fun projection(columns: List<String>): Frame = object : Frame {

    override fun schema(): Schema {
      val newColumns = outer().schema().fields.filter { columns.contains(it.name) }
      return Schema(newColumns)
    }

    override fun observable(): Observable<Row> {

      val oldSchema = outer().schema()
      val newSchema = schema()

      return outer().observable().map { row ->
        val values = newSchema.columnNames().map {
          val k = oldSchema.indexOf(it)
          row.values[k]
        }
        Row(newSchema, values)
      }
    }
  }

  /**
   * Execute a side effecting function for every row in the frame, returning the same Frame.
   *
   * @param fn the function to execute
   * @return this frame, to allow for builder style chaining
   */
  fun <U> foreach(fn: (Row) -> U): Frame = object : Frame {
    override fun schema(): Schema = outer().schema()
    override fun observable(): Observable<Row> = outer().observable().map {
      fn(it)
      it
    }
  }

  fun drop(k: Int): Frame = object : Frame {
    override fun schema(): Schema = outer().schema()
    override fun observable(): Observable<Row> = outer().observable().skip(k)
  }

  fun map(f: (Row) -> Row): Frame = object : Frame {
    override fun observable(): Observable<Row> = outer().observable().map(f)
    override fun schema(): Schema = outer().schema()
  }

  fun filterNot(p: (Row) -> Boolean): Frame = filter { str -> !p(str) }

  fun filter(p: (Row) -> Boolean): Frame = object : Frame {
    override fun schema(): Schema = outer().schema()
    override fun observable(): Observable<Row> = outer().observable().filter(p)
  }

  /**
   * Filters where the given column matches the given predicate.
   */
  fun filter(columnName: String, p: (Any?) -> Boolean): Frame = object : Frame {
    override fun schema(): Schema = outer().schema()
    override fun observable(): Observable<Row> {
      val index = schema().indexOf(columnName)
      return outer().observable().filter { p(it.values[index]) }
    }
  }

  fun dropNullRows(): Frame = object : Frame {
    override fun observable(): Observable<Row> = outer().observable().filter { !it.values.contains(null) }
    override fun schema(): Schema = outer().schema()
  }

  // -- actions --
  fun <A> fold(a: A, fn: (A, Row) -> A): A = FoldPlan.execute(this, a, fn)

  fun forall(p: (Row) -> Boolean): Boolean = ForallPlan.execute(this, p)
  fun exists(p: (Row) -> Boolean): Boolean = ExistsPlan.execute(this, p)
  fun find(p: (Row) -> Boolean): Row? = FindPlan.execute(this, p)
  fun head(): Row? = HeadPlan.execute(this)

  fun to(sink: Sink): Long = SinkPlan.execute(sink, this)
  fun size(): Long = ToSizePlan.execute(this)
  fun counts(): Map<String, Content.Counts> = CountsPlan.execute(this)
  fun toList(): List<Row> = ToSeqPlan.execute(this)
  fun toSet(): Set<Row> = ToSetPlan.execute(this)

  companion object {
    operator fun invoke(_schema: Schema, vararg rows: Row): Frame = invoke(_schema, rows.asList())
    operator fun invoke(_schema: Schema, rows: List<Row>): Frame = object : Frame {
      override fun schema(): Schema = _schema
      override fun observable(): Observable<Row> = Observable.from(rows)
    }
  }
}

object FoldPlan {
  fun <A> execute(frame: Frame, a: A, p: (A, Row) -> A): A = throw RuntimeException()
}

object ForallPlan {
  fun execute(frame: Frame, p: (Row) -> Boolean): Boolean = false
}

object ExistsPlan {
  fun execute(frame: Frame, p: (Row) -> Boolean): Boolean = false
}

object FindPlan {
  fun execute(frame: Frame, p: (Row) -> Boolean): Row? = null
}

object HeadPlan {
  fun execute(frame: Frame): Row? = null
}

object SinkPlan {
  fun execute(sink: Sink, frame: Frame): Long = 0
}

object CountsPlan {
  fun execute(frame: Frame): Map<String, Content.Counts> = mapOf()
}

object ToSizePlan {
  fun execute(frame: Frame): Long = 0
}

object ToSeqPlan {
  fun execute(frame: Frame): List<Row> = listOf()
}

object ToSetPlan {
  fun execute(frame: Frame): Set<Row> = setOf()
}