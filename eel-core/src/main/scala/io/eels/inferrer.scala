package io.eels

import io.eels.schema._

trait SchemaInferrer {
  def schemaOf(headers: List[String]): StructType
}

object SchemaInferrer {
  def apply(default: DataType, first: DataTypeRule, rest: DataTypeRule*): SchemaInferrer = new SchemaInferrer {
    override def schemaOf(headers: List[String]): StructType = {

      val fields = headers.map { header =>
        (first +: rest).foldLeft(None: Option[Field]) { case (field, rule) =>
          field.orElse(rule(header))
        }.getOrElse(Field(header, default))
      }

      StructType(fields)
    }
  }
}

object StringInferrer extends SchemaInferrer {
  override def schemaOf(headers: List[String]): StructType = StructType(headers.map { header =>
    Field(header, StringType, true)
  })
}

case class DataTypeRule(pattern: String, dataType: DataType, nullable: Boolean = true) {
  def apply(header: String): Option[Field] = {
    if (header.matches(pattern)) Some(Field(header, dataType, nullable)) else None
  }
}