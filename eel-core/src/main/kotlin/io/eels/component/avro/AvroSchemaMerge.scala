package io.eels.component.avro

import org.apache.avro.Schema

@deprecated("use the functionality from avro4s when we can ditch 2.10 in eel")
object AvroSchemaMerge {

  def apply(name: String, namespace: String, schemas: Seq[Schema]): Schema = {
    require(schemas.forall(_.getType == Schema.Type.RECORD), "Can only merge records")

    val doc = schemas.flatMap(x => Option(x.getDoc)).mkString("; ")
    val fields = schemas.flatMap(_.getFields.asScala)

    // should try to keep order, by using the order of the first schema, then second second, etc, taking the
    // first field as found for each field name
    // note: must use ListMap as it keeps insertion order, otherwise this whole endeavour is pointless
    val mergedFields = fields.foldLeft(ListMap.empty[String, Schema.Field]) { (map, field) =>
      if (map.contains(field.name)) map
      else map + (field.name -> new Schema.Field(field.name, field.schema, field.doc, field.defaultValue))
    }

    val schema = Schema.createRecord(name, if (doc.isEmpty) null else doc, namespace, false)
    schema.setFields(mergedFields.values.toList.asJava)
    schema
  }
}

object q