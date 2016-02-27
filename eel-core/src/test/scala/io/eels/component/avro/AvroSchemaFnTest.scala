package io.eels.component.avro

import io.eels.{Column, Schema, SchemaType}
import org.apache.avro.{Schema => AvroSchema}
import org.codehaus.jackson.node.NullNode
import org.scalatest.{Matchers, WordSpec}

class AvroSchemaFnTest extends WordSpec with Matchers {

  "AvroSchemaFn" should {
    "convert to avro schema using unions for nulls with null default value" in {

      val schema = Schema(List(
        Column("a", SchemaType.String, true)
      ))

      val fields = AvroSchemaFn.toAvro(schema).getFields
      fields.get(0).defaultValue() shouldBe NullNode.getInstance()
      fields.get(0).schema.getType shouldBe AvroSchema.Type.UNION
      fields.get(0).schema.getTypes.get(0).getType shouldBe AvroSchema.Type.NULL
      fields.get(0).schema.getTypes.get(1).getType shouldBe AvroSchema.Type.STRING
    }
    "convert to avro schema without default value" in {

      val schema = Schema(List(
        Column("a", SchemaType.Int, false)
      ))

      val fields = AvroSchemaFn.toAvro(schema).getFields
      println(AvroSchemaFn.toAvro(schema).toString(true))
      fields.get(0).defaultValue() shouldBe null
      fields.get(0).schema.getType shouldBe AvroSchema.Type.INT
    }
  }
}
