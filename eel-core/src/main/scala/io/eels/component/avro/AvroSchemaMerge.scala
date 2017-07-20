package io.eels.component.avro

import com.sksamuel.exts.StringOption
import org.apache.avro.Schema

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object AvroSchemaMerge {

  def apply(name: String, namespace: String, schemas: List[Schema]): Schema = {
    require(schemas.forall(_.getType == Schema.Type.RECORD), "Can only merge records")

    // documentations can just be a concat
    val doc = schemas.map(_.getDoc).filter(_ != null).mkString("; ")

    // simple impl to start: take all the fields from the first schema, and then add in the missing ones
    // from second 2 and so on
    val fields = new ArrayBuffer[Schema.Field]()
    schemas.foreach { schema =>
      schema.getFields.asScala.filterNot { field => fields.exists(_.name() == field.name) }.foreach { field =>
        // avro is funny about sharing fields, so need to copy it
        val copy = new Schema.Field(field.name(), field.schema(), StringOption(field.doc).orNull, field.defaultVal)
        fields.append(copy)
      }
    }

    val schema = Schema.createRecord(name, if (doc.isEmpty()) null else doc, namespace, false)
    schema.setFields(fields.result().asJava)
    schema
  }
}