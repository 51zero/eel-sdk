package io.eels.schema

case class Field(name: String,
                 `type`: FieldType = FieldType.String,
                 nullable: Boolean = true,
                 precision: Precision = Precision(0),
                 scale: Scale = Scale(0),
                 signed: Boolean = false,
                 arrayType: Option[FieldType] = None, // if an array then the type of the array elements
                 fields: Seq[Field] = Nil, // if a struct, then the fields of that struct
                 partition: Boolean = false,
                 comment: Option[String] = None) {

  // Creates a lowercase version of this column
  def toLowerCase(): Field = copy(name = name.toLowerCase())

  def withComment(comment: String): Field = copy(comment = Some(comment))
  def withNullable(nullable: Boolean): Field = copy(nullable = nullable)
  def withPartition(partition: Boolean) = copy(partition = partition)
}


object Field {
  def createStruct(name: String, first: Field, rest: Field*): Field = createStruct(name, first +: rest)
  def createStruct(name: String, fields: Seq[Field]): Field = Field(name, `type` = FieldType.Struct, fields = fields)
}

case class Precision(value: Int) extends AnyVal
case class Scale(value: Int) extends AnyVal