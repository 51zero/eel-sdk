package io.eels.component.avro

import io.eels.{Column, FrameSchema, SchemaType}
import org.apache.avro.Schema
import org.scalatest.{Matchers, WordSpec}

class AvroSchemaFnTest extends WordSpec with Matchers {

  "AvroSchemaFn" should {
    "convert to avro schema using unions for nulls" in {
      val schema = FrameSchema(List(
        Column("a", SchemaType.String, true),
        Column("b", SchemaType.Int, false)
      ))
      val fields = AvroSchemaFn.toAvro(schema).getFields
      println(AvroSchemaFn.toAvro(schema).toString(true))
      fields.get(0).defaultValue() shouldBe null
      fields.get(0).schema.getType shouldBe Schema.Type.UNION
      fields.get(0).schema.getTypes.get(0).getType shouldBe Schema.Type.NULL
      fields.get(0).schema.getTypes.get(1).getType shouldBe Schema.Type.STRING

      fields.get(1).defaultValue() shouldBe null
      fields.get(1).schema.getType shouldBe Schema.Type.INT
    }
  }
}
