package io.eels.schema

data class Field(val name: String,
                 val type: FieldType = FieldType.String,
                 val nullable: Boolean = true,
                 val precision: Precision = Precision(0),
                 val scale: Scale = Scale(0),
                 val signed: Boolean = false,
                 val arrayType: FieldType? = null, // if an array then the type of the array elements
                 val fields: List<Field> = emptyList(), // if a struct, then the fields of that struct
                 val comment: String? = null) {
  // Creates a lowercase version of this column
  fun toLowerCase(): Field = copy(name = name.toLowerCase())

  companion object {
    fun createStruct(name: String, vararg fields: Field): Field = Field(name, type = FieldType.Struct, fields = fields.asList())
  }
}

enum class FieldType {
  BigInt,
  Binary,
  Boolean,
  Date,
  Decimal,
  Double,
  Float,
  Int,
  Long,
  Short,
  String,
  Struct,
  Timestamp
}

data class Precision(val value: Int)
data class Scale(val value: Int)