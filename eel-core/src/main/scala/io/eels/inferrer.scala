package io.eels

import io.eels.schema.{Field, FieldType, Schema}

trait SchemaInferrer {
  def schemaOf(headers: List[String]): Schema
}

object SchemaInferrer {
  def apply(default: FieldType, first: SchemaRule, rest: SchemaRule*): SchemaInferrer = new SchemaInferrer {
    override def schemaOf(headers: List[String]): Schema = {

      val fields = headers.map { header =>
        (first +: rest).foldLeft(None: Option[Field]) { case (field, rule) =>
          field.orElse(rule(header))
        }.getOrElse(Field(header, default))
      }

      Schema(fields)
    }
  }
}

object StringInferrer extends SchemaInferrer {
  override def schemaOf(headers: List[String]): Schema = Schema(headers.map { header =>
    Field(header, FieldType.String, true)
  })
}

case class SchemaRule(pattern: String, fieldType: FieldType, nullable: Boolean = true) {
  def apply(header: String): Option[Field] = {
    if (header.matches(pattern)) Some(Field(header, fieldType, nullable)) else None
  }
}