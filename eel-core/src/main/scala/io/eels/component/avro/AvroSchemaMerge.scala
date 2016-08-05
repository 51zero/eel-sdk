package io.eels.component.avro

import org.apache.avro.Schema

import scala.collection.JavaConverters._

object AvroSchemaMerge {

  def apply(name: String, namespace: String, schemas: List[Schema]): Schema = {
    require(schemas.forall(_.getType == Schema.Type.RECORD), "Can only merge records")

    // documentations can just be a concat
    val doc = schemas.map(_.getDoc).filter(_ != null).mkString("; ")

    val schema = Schema.createRecord(name, if (doc.isEmpty()) null else doc, namespace, false)
    val fields = schemas.flatMap(_.getFields.asScala).groupBy(_.name).map { case (name, fields) =>

      // documentations can just be a concat
      val fieldDoc = fields.map(_.doc).filterNot(_ == null).mkString("; ")
      val default = fields.map(_.defaultValue).find(_ != null).orNull

      // simple impl to start, just take the first field
      val merged = fields.head
      new Schema.Field(name, merged.schema(), if (fieldDoc.isEmpty()) null else fieldDoc, default)
    }
    schema.setFields(fields.toList.asJava)
    schema
  }
}