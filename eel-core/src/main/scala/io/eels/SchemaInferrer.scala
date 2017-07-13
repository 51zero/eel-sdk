package io.eels

import io.eels.schema._

trait SchemaInferrer {
  def infer(name: String): Field
  def struct(headers: List[String]): StructType
}

class BasicSchemaInferrer(default: DataType, rules: Seq[DataTypeRule]) extends SchemaInferrer {

  override def struct(headers: List[String]): StructType = {
    val fields = headers.map(infer)
    StructType(fields)
  }

  override def infer(name: String): Field = {
    rules.foldLeft(None: Option[Field]) { case (field, rule) =>
      field.orElse(rule(name))
    }.getOrElse(Field(name, default))
  }
}

object SchemaInferrer {
  def apply(default: DataType, first: DataTypeRule, rest: DataTypeRule*): SchemaInferrer = apply(default, first +: rest)
  def apply(default: DataType, rules: Seq[DataTypeRule]): SchemaInferrer = new BasicSchemaInferrer(default, rules)
}

object StringInferrer extends BasicSchemaInferrer(StringType, Nil)

case class DataTypeRule(pattern: String, dataType: DataType, nullable: Boolean = true) {
  def apply(header: String): Option[Field] = {
    if (header.matches(pattern)) Some(Field(header, dataType, nullable)) else None
  }
}