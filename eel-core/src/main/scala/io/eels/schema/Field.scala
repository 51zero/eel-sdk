package io.eels.schema

// a named type
case class Field(name: String,
                 dataType: DataType = StringType,
                 nullable: Boolean = true,
                 partition: Boolean = false,
                 comment: Option[String] = None,
                 key: Boolean = false, // for schemas that support a primary key,
                 metadata: Map[String, String] = Map.empty) {

  // returns the default value for this field or throws an exception
  // if the field has no default and is not nullable
  def default: Any = if (nullable) null else sys.error(s"Not nullable field $this")

  // Creates a lowercase copy of this field
  def toLowerCase(): Field = copy(name = name.toLowerCase())

  def withComment(comment: String): Field = copy(comment = Option(comment))
  def withNullable(nullable: Boolean): Field = copy(nullable = nullable)
  def withPartition(partition: Boolean) = copy(partition = partition)
}

object Field {
  def createStructField(name: String, first: Field, rest: Field*): Field = createStructField(name, first +: rest)
  def createStructField(name: String, fields: Seq[Field]): Field = Field(name, dataType = StructType(fields.toList))
}