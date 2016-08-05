package io.eels;

trait Frame {

  val defaultBufferSize: Int = 1000

  def outer() = this

  def schema(): Schema

  /**
    * Returns an Observable which can be subscribed to in order to receieve all
    * the rows held by this Frame.
    */
  def rows(): Observable < Row >

  /**
    * Combines two frames together such that the columns from this frame are joined with the columns
    * of the given frame. Eg, if this frame has A,B and the given frame has C,D then the result will
    * be A,B,C,D
    */
  def join(other: Frame): Frame = new Frame {
    override def schema(): Schema = outer().schema().join(other.schema())

    override def rows(): Observable < Row > {
      val schema = schema ()
      val func: (Row, Row) -> Row = {a, b -> Row (schema, a.values.plus (b.values) )}
      return outer ().rows ().zipWith (other.rows (), func)
    }
  }

  /**
    * Foreach row, any values that match "from" will be replaced with "target".
    * This operation applies to all values for all rows.
    */
  def replace(from: String, target: Any ?): Frame = new Frame {
    override def schema(): Schema = outer().schema()
    override def rows(): Observable < Row > = outer().rows().map {
      val values = it.values.map {
        if (it == from) target else it
      }
      Row(it.schema, values)
    }
  }

  /**
    * Replaces any values that match "form" with the value "target".
    * This operation only applies to the column name specified.
    */
  def replace(columnName: String, from: String, target: Any ?): Frame = new Frame {
    override def schema(): Schema = outer().schema()
    override def rows(): Observable < Row > {
      val index = schema ().indexOf (columnName)
      return outer ().rows ().map {
      val values = it.values.toMutableList ()
      if (values[index] == from)
      values[index] = target
      Row (it.schema, values.toList () )
    }
    }
  }

  /**
    * For each row, the value corresponding to the given fieldName is applied to the function.
    * The result of the function is the new value for that cell.
    */
  def replace(columnName: String, fn: (Any ?) -> Any ?): Frame = new Frame {
    override def schema(): Schema = outer().schema()
    override def rows(): Observable < Row > {
      val index = schema ().indexOf (columnName)
      return outer ().rows ().map {
      val values = it.values.toMutableList ()
      values[index] = fn (values[index] )
      Row (it.schema, values.toList () )
    }
    }
  }

  def takeWhile(pred: (Row) -> Boolean): Frame = new Frame {
    override def schema(): Schema = outer().schema()
    override def rows(): Observable < Row > = outer().rows().takeWhile(pred)
  }

  def takeWhile(columnName: String, pred: (Any ?) -> Boolean): Frame = new Frame {
    override def schema(): Schema = outer().schema()
    override def rows(): Observable < Row > {
      val index = outer ().schema ().indexOf (columnName)
      return outer ().rows ().takeWhile {pred (it.values[index] )}
    }
  }

  def updateColumnType(columnName: String, fieldType: FieldType): Frame = new Frame {
    override def schema(): Schema = outer().schema().updateFieldType(columnName, fieldType)
    override def rows(): Observable < Row > = outer().rows()
  }

  def dropWhile(pred: (Row) -> Boolean): Frame = new Frame {
    override def schema(): Schema = outer().schema()
    override def rows(): Observable < Row > = outer().rows().skipWhile(pred)
  }

  def dropWhile(columnName: String, pred: (Any ?) -> Boolean): Frame = new Frame {
    override def schema(): Schema = outer().schema()
    override def rows(): Observable < Row > {
      val index = outer ().schema ().indexOf (columnName)
      return outer ().rows ().skipWhile {pred (it.values[index] )}
    }
  }

  /**
    * Returns a new Frame where only each "k" row is retained. Ie, if sample is 2, then on average,
    * every other row will be returned. If sample is 10 then only 10% of rows will be returned.
    * When running concurrently, the rows that are sampled will vary depending on the ordering that the
    * workers pull through the rows. Each stream (thread) uses its own count for the sample.
    */
  def sample(k: Int): Frame = new Frame {
    override def schema(): Schema = outer().schema()
    override def rows(): Observable < Row > = outer().rows()
  }

  /**
    * Returns a new Frame with the new column of type String added at the end. The value of
    * this column for each Row is specified by the default value.
    */
  def addColumn(name: String, defaultValue: String): Frame = addColumn(Field(name), defaultValue)

  /**
    * Returns a new Frame with the given column added at the end. The value of this column
    * for each Row is specified by the default value. The value must be compatible with the
    * Column definition. Eg, an error will occur if the Column had type Int and the default
    * value was 1.3
    */
  def addColumn(field: Field, defaultValue: Any): Frame = new Frame {
    override def schema(): Schema = outer().schema().addField(field)
    override def rows(): Observable < Row > {
      val exists = outer ().schema ().fieldNames ().contains (field.name)
      if (exists) throw IllegalArgumentException ("Cannot add column $field as it already exists")
      val newSchema = schema ()
      return outer ().rows ().map {Row (newSchema, it.values.plus (defaultValue) )}
    }
  }

  def addColumnIfNotExists(name: String, defaultValue: Any): Frame = addColumnIfNotExists(Field(name), defaultValue)

  def addColumnIfNotExists(field: Field, defaultValue: Any): Frame = new Frame {
    override def schema(): Schema = outer().schema().addFieldIfNotExists(field)
    override def rows(): Observable < Row > {
      val exists = outer ().schema ().fieldNames ().contains (field.name)
      return if (exists) outer ().rows () else addColumn (field, defaultValue).rows ()
    }
  }

  def removeColumn(columnName: String, caseSensitive: Boolean = true): Frame = new Frame {
    override def schema(): Schema = outer().schema().removeField(columnName, caseSensitive)
    override def rows(): Observable < Row > {
      val index = outer ().schema ().indexOf (columnName, caseSensitive)
      val newSchema = schema ()
      return outer ().rows ().map {
      val newValues = it.values.slice (0..index).plus (it.values.slice (index + 1..it.values.size) )
      Row (newSchema, newValues)
    }
    }
  }

  def updateColumn(field: Field): Frame = new Frame {
    override def rows(): Observable < Row > {
      throw UnsupportedOperationException ()
    }

    override def schema(): Schema = outer().schema().replaceField(field.name, field)

    //    override def buffer(): Buffer = new  Buffer {
    //      val buffer = outer().buffer()
    //      val index = outer().schema().indexOf(column)
    //      override def close(): Unit = buffer.close()
    //      override def stream(): Observable<Row> = buffer.stream()
    //    }
  }

  def renameColumn(nameFrom: String, nameTo: String): Frame = new Frame {
    override def schema(): Schema = outer().schema().renameField(nameFrom, nameTo)
    override def rows(): Observable < Row > = outer().rows()
  }

  def stripFromColumnName(chars: List < Char >): Frame = new Frame {
    override def schema(): Schema = outer().schema().stripFromFieldNames(chars)
    override def rows(): Observable < Row > = outer().rows()
  }

  def explode(fn: (Row) -> List < Row >): Frame = new Frame {
    override def rows(): Observable < Row > = outer().rows().flatMap {
      Observable.from(fn(it))
    }
    override def schema(): Schema = outer().schema()
  }

  def fill(defaultValue: String): Frame = new Frame {
    override def rows(): Observable < Row > = rows().map {
      val newValues = it.values.map {
        when(it) {
          null -> defaultValue
          else -> it
        }
      }
      Row(it.schema, newValues)
    }

    override def schema(): Schema = outer().schema()
  }

  /**
    * Joins two frames together, such that the elements of the given frame are appended to the
    * end of this frame. This operation is the same as a concat operation.
    */
  def union(other: Frame): Frame = new Frame {
    // todo check schemas are compatible
    override def schema(): Schema = outer().schema()

    override def rows(): Observable < Row > = outer().rows().concatWith(other.rows())
  }

  def projectionExpression(expr: String): Frame = projection(expr.split(',').map {
    it.trim()
  })
  def projection(vararg
  columns: String): Frame = projection(columns.asList())

  /**
    * Returns a new frame which contains the given list of columns from the existing frame.
    */
  def projection(columns: List < String >): Frame = new Frame {

    override def schema(): Schema {
      val newColumns = outer ().schema ().fields.filter {columns.contains (it.name)}
      return Schema (newColumns)
    }

    override def rows(): Observable < Row > {

      val oldSchema = outer ().schema ()
      val newSchema = schema ()

      return outer ().rows ().map {row ->
      val values = newSchema.fieldNames ().map {
      val k = oldSchema.indexOf (it)
      row.values[k]
    }
      Row (newSchema, values)
    }
    }
  }

  /**
    * Execute a side effecting function for every row in the frame, returning the same Frame.
    *
    * @param fn the function to execute
    * @return this frame, to allow for builder style chaining
    */
  def <U>foreach(fn: (Row) -> U): Frame = new Frame
    {override def schema(): Schema = outer().schema()
  override def rows(): Observable < Row > = outer().rows().map {
    fn(it)
    it
  }}

  def drop(k: Int): Frame = new Frame
  {override def schema(): Schema = outer().schema()
  override def rows(): Observable < Row > = outer().rows().skip(k)}

  def map(f: (Row) -> Row): Frame = new Frame
  {override def rows(): Observable < Row > = outer().rows().map(f)
  override def schema(): Schema = outer().schema()}

  def filterNot(p: (Row) -> Boolean): Frame = filter
  {str -> !p(str)}

  def filter(p: (Row) -> Boolean): Frame = new Frame
  {override def schema(): Schema = outer().schema()
  override def rows(): Observable < Row > = outer().rows().filter(p)}

  /**
  * Filters where the given column matches the given predicate.
  */
  def filter(columnName: String, p: (Any?) -> Boolean): Frame = new Frame
  {override def schema(): Schema = outer().schema()
  override def rows(): Observable < Row > {
    val index = schema ().indexOf (columnName)
    return outer ().rows ().filter {p (it.values[index] )}
  }}

  def dropNullRows(): Frame = new Frame
  {override def rows(): Observable < Row > = outer().rows().filter {
    !it.values.contains(null)
  }
  override def schema(): Schema = outer().schema()}

  // -- actions --
  def
  <A>fold(a: A, fn: (A, Row) -> A): A = rows().reduce(a,
    {a, row -> fn(a, row)}
  ).toBlocking().single()

  def forall(p: (Row) -> Boolean): Boolean = ForallPlan.execute(this, p)
  def exists(p: (Row) -> Boolean): Boolean = rows().exists
  {p(it)}
  .toBlocking().single()
  def find(p: (Row) -> Boolean): Row? = rows().first
  {p(it)}
  .toBlocking().single()
  def head(): Row? = rows().first().toBlocking().single()

  def to(sink: Sink): Long = SinkPlan.execute(sink, this)
  def size(): Int = rows().count().toBlocking().single()
  def counts(): Map
  <String, Content.Counts> = CountsPlan.execute(this)
    def toList(): List
    <Row>= rows().toList().toBlocking().single()
      def toSet(): Set
      <Row>= toList().toSet()

        companion object
        {def create(schema: Schema, values: List < List < Any ?>>) = invoke(schema, values.map {
        Row(schema, it)
      })
      operator def invoke(schema: Schema, vararg rows: List < Any ?>) = invoke(schema, rows.map {
    Row(schema, it)
  })
  operator def invoke(_schema: Schema, vararg rows: Row): Frame = invoke(_schema, rows.asList())
  operator def invoke(_schema: Schema, rows: List < Row >): Frame = new Frame {
    override def schema(): Schema = _schema
    override def rows(): Observable < Row > = Observable.from(rows)
  }}
  }
