package io.eels.component.avro

import java.util

import io.eels.{Column, Schema, SchemaType}
import org.apache.avro.{SchemaBuilder, Schema => AvroSchema}
import org.codehaus.jackson.node.NullNode
import org.scalatest.{Matchers, WordSpec}

class AvroSchemaFnTest extends WordSpec with Matchers {

  "AvroSchemaFn.toAvro" should {
    "use a union of [null, type] for a nullable column" in {

      val schema = Schema(List(
        Column("a", SchemaType.String, true)
      ))

      val fields = AvroSchemaFn.toAvro(schema).getFields
      fields.get(0).schema.getType shouldBe AvroSchema.Type.UNION
      fields.get(0).schema.getTypes.get(0).getType shouldBe AvroSchema.Type.NULL
      fields.get(0).schema.getTypes.get(1).getType shouldBe AvroSchema.Type.STRING
    }
    "set default type of NullNode for a nullable column" in {

      val schema = Schema(List(
        Column("a", SchemaType.String, true)
      ))

      val fields = AvroSchemaFn.toAvro(schema).getFields
      fields.get(0).defaultValue() shouldBe NullNode.getInstance()
    }
    "not set a default value for a non null column" in {

      val schema = Schema(List(
        Column("a", SchemaType.Int, false)
      ))

      val fields = AvroSchemaFn.toAvro(schema).getFields
      fields.get(0).defaultValue() shouldBe null
      fields.get(0).schema.getType shouldBe AvroSchema.Type.INT
    }
  }

  "AvroSchemaFn.fromAvro" should {
    "convert avro unions [null, string] to nullable columns" in {
      val avro = SchemaBuilder.record("dummy").fields().optionalString("str").endRecord()
      AvroSchemaFn.fromAvro(avro) shouldBe Schema(List(Column("str", SchemaType.String, true, 0, 0, true, None)))
    }
    "convert avro unions [null, enum] to nullable columns" in {
      val enum = AvroSchema.createEnum("myenum", null, null, util.Arrays.asList("a", "b"))
      val union = AvroSchema.createUnion(util.Arrays.asList(enum, AvroSchema.create(AvroSchema.Type.NULL)))
      val avro = SchemaBuilder.record("dummy").fields().name("u").`type`(union).noDefault().endRecord()
      AvroSchemaFn.fromAvro(avro) shouldBe Schema(List(Column("u", SchemaType.String, true, 0, 0, true, None)))
    }
    "convert avro unions [null, double] to nullable double columns" in {
      val union = AvroSchema.createUnion(util.Arrays.asList(SchemaBuilder.builder().doubleType(), SchemaBuilder.builder().nullType()))
      val avro = SchemaBuilder.record("dummy").fields().name("u").`type`(union).noDefault().endRecord()
      AvroSchemaFn.fromAvro(avro) shouldBe Schema(List(Column("u", SchemaType.Double, true, 0, 0, true, None)))
    }
    "convert avro boolean fields" in {
      val avro = SchemaBuilder.record("dummy").fields().requiredBoolean("b").endRecord()
      AvroSchemaFn.fromAvro(avro) shouldBe Schema(List(Column("b", SchemaType.Boolean, false, 0, 0, true, None)))
    }
    "convert avro float fields" in {
      val avro = SchemaBuilder.record("dummy").fields().requiredFloat("f").endRecord()
      AvroSchemaFn.fromAvro(avro) shouldBe Schema(List(Column("f", SchemaType.Float, false, 0, 0, true, None)))
    }
    "convert avro double fields" in {
      val avro = SchemaBuilder.record("dummy").fields().requiredDouble("d").endRecord()
      AvroSchemaFn.fromAvro(avro) shouldBe Schema(List(Column("d", SchemaType.Double, false, 0, 0, true, None)))
    }
    "convert avro fixed fields to string" in {
      val fixed = AvroSchema.createFixed("dummy", null, null, 123)
      val avro = SchemaBuilder.record("dummy").fields().name("f").`type`(fixed).noDefault().endRecord()
      AvroSchemaFn.fromAvro(avro) shouldBe Schema(List(Column("f", SchemaType.String, false, 0, 0, true, None)))
    }
    "convert avro enum fields to string" in {
      val enum = AvroSchema.createEnum("myenum", null, null, util.Arrays.asList("a", "b"))
      val avro = SchemaBuilder.record("dummy").fields().name("e").`type`(enum).noDefault().endRecord()
      AvroSchemaFn.fromAvro(avro) shouldBe Schema(List(Column("e", SchemaType.String, false, 0, 0, true, None)))
    }
    "convert avro int fields" in {
      val avro = SchemaBuilder.record("dummy").fields().requiredInt("i").endRecord()
      AvroSchemaFn.fromAvro(avro) shouldBe Schema(List(Column("i", SchemaType.Int, false, 0, 0, true, None)))
    }
    "convert avro long fields" in {
      val avro = SchemaBuilder.record("dummy").fields().requiredLong("l").endRecord()
      AvroSchemaFn.fromAvro(avro) shouldBe Schema(List(Column("l", SchemaType.Long, false, 0, 0, true, None)))
    }
    "convert avro string fields" in {
      val avro = SchemaBuilder.record("dummy").fields().requiredString("s").endRecord()
      AvroSchemaFn.fromAvro(avro) shouldBe Schema(List(Column("s", SchemaType.String, false, 0, 0, true, None)))
    }
  }
}
