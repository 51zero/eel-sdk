package io.eels.component.avro

import org.apache.avro.Schema

object AvroSchemaMerge {

  operator fun invoke(name: String, namespace: String, schemas: List<Schema>): Schema {
    require(schemas.all { it.type == Schema.Type.RECORD }, { "Can only merge records" })

    // documentations can just be a concat
    val doc = schemas.map { it.doc }.filterNotNull().joinToString("; ")

    return Schema.createRecord(name, if (doc.isEmpty()) null else doc, namespace, false).apply {
      val fields = schemas.flatMap { it.fields }.groupBy { it.name() }.map {

        // documentations can just be a concat
        val fieldDoc = it.value.map { it.doc() }.filterNotNull().joinToString("; ")
        val default = it.value.find { it.defaultValue() != null }?.defaultValue()

        // simple impl to start, just take the first field
        val merged = it.value.first()
        Schema.Field(it.key, merged.schema(), if (fieldDoc.isEmpty()) null else fieldDoc, default)
      }
      setFields(fields)
    }
  }
}