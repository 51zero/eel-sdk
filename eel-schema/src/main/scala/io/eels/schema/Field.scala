package io.eels.schema

// a named type
case class Field(name: String,
                 dataType: DataType = StringType,
                 nullable: Boolean = true,
                 partition: Boolean = false,
                 comment: Option[String] = None,
                 key: Boolean = false, // for schemas that support a primary key,
                 defaultValue: Any = null, // the default value, null can only be a default if nullable is also set to true
                 metadata: Map[String, String] = Map.empty,
                 columnFamily: Option[String] = None) {

  // returns the default value for this field or throws an exception
  // if the field has no default and is not nullable
  def default: Any = {
    if (defaultValue != null) defaultValue
    else if (defaultValue == null && nullable) null
    else sys.error("Default value is null, but field is not nullable")
  }

  // Creates a lowercase copy of this field
  def toLowerCase(): Field = copy(name = name.toLowerCase())

  def withComment(comment: String): Field = copy(comment = Option(comment))

  def withNullable(nullable: Boolean): Field = copy(nullable = nullable)

  def withPartition(partition: Boolean): Field = copy(partition = partition)

  def withColumnFamily(columnFamily: String): Field = copy(columnFamily = Option(columnFamily))
}

object Field {
  def createStructField(name: String, first: Field, rest: Field*): Field = createStructField(name, first +: rest)

  def createStructField(name: String, fields: Seq[Field]): Field = Field(name, dataType = StructType(fields.toList))
}