package io.eels.schema

// accepts multiple schemas and generates the superset of those schemas
object SchemaMerger {
  def apply(schemas: Seq[StructType]): StructType = {
    def merge(a: StructType, b: StructType): StructType = {
      // firstly we add all the fields from b that don't exist in a, if a field does
      // already exist then it must be the same schema type.
      // if the field doesn't exist then it must be set to null so its compatible with the other schema
      b.fields.foldLeft(a)((schema, field) => schema.addFieldIfNotExists(field))
    }
    schemas.reduceLeft((a, b) => merge(a, b))
  }
}
