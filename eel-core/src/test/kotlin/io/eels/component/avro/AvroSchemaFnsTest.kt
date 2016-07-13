package io.eels.component.avro

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import org.apache.avro.SchemaBuilder

import io.eels.schema.Precision
import io.eels.schema.Scale
import io.kotlintest.specs.WordSpec
import org.codehaus.jackson.node.NullNode
import java.util.*

class AvroSchemaFnTest : WordSpec() {

  init {
    "toAvro" should {
      "use a union of [null, type] for a nullable column" {
        val schema = Schema(Field("a", FieldType.String, true))
        val fields = toAvroSchema(schema).fields
        fields[0].schema().type shouldBe org.apache.avro.Schema.Type.UNION
        fields[0].schema().types[0].type shouldBe org.apache.avro.Schema.Type.NULL
        fields[0].schema().types[1].type shouldBe org.apache.avro.Schema.Type.STRING
      }
      "set default type of NullNode for a nullable column" {
        val schema = Schema(Field("a", FieldType.String, true))
        val fields = toAvroSchema(schema).fields
        fields[0].defaultValue() shouldBe NullNode.getInstance()
      }
      "not set a default value for a non null column" {
        val schema = Schema(Field("a", FieldType.Int, false))
        val fields = toAvroSchema(schema).fields
        (fields[0].defaultValue() == null) shouldBe true
        fields[0].schema().type shouldBe org.apache.avro.Schema.Type.INT
      }
    }

    "fromAvro" should {
      "convert avro unions [null, string] to nullable columns" {
        val avro = SchemaBuilder.record("dummy").fields().optionalString("str").endRecord()
        fromAvroSchema(avro) shouldBe Schema(Field("str", FieldType.String, true, Precision(0), Scale(0), false))
      }
      "convert avro unions [null, enum] to nullable columns" {
        val enum = org.apache.avro.Schema.createEnum("myenum", null, null, Arrays.asList("a", "b"))
        val union = org.apache.avro.Schema.createUnion(Arrays.asList(enum, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL)))
        val avro = SchemaBuilder.record("dummy").fields().name("u").`type`(union).noDefault().endRecord()
        fromAvroSchema(avro) shouldBe Schema(Field("u", FieldType.String, true, Precision(0), Scale(0), false))
      }
      "convert avro unions [null, double] to nullable double columns" {
        val union = org.apache.avro.Schema.createUnion(Arrays.asList(SchemaBuilder.builder().doubleType(), SchemaBuilder.builder().nullType()))
        val avro = SchemaBuilder.record("dummy").fields().name("u").`type`(union).noDefault().endRecord()
        fromAvroSchema(avro) shouldBe Schema(Field("u", FieldType.Double, true, Precision(0), Scale(0), false))
      }
      "convert avro boolean fields" {
        val avro = SchemaBuilder.record("dummy").fields().requiredBoolean("b").endRecord()
        fromAvroSchema(avro) shouldBe Schema(Field("b", FieldType.Boolean, false, Precision(0), Scale(0), false))
      }
      "convert avro float fields" {
        val avro = SchemaBuilder.record("dummy").fields().requiredFloat("f").endRecord()
        fromAvroSchema(avro) shouldBe Schema(Field("f", FieldType.Float, false, Precision(0), Scale(0), false))
      }
      "convert avro double fields" {
        val avro = SchemaBuilder.record("dummy").fields().requiredDouble("d").endRecord()
        fromAvroSchema(avro) shouldBe Schema(Field("d", FieldType.Double, false, Precision(0), Scale(0), false))
      }
      "convert avro fixed fields to string" {
        val fixed = org.apache.avro.Schema.createFixed("dummy", null, null, 123)
        val avro = SchemaBuilder.record("dummy").fields().name("f").`type`(fixed).noDefault().endRecord()
        fromAvroSchema(avro) shouldBe Schema(Field("f", FieldType.String, false, Precision(0), Scale(0), false))
      }
      "convert avro enum fields to string" {
        val enum = org.apache.avro.Schema.createEnum("myenum", null, null, Arrays.asList("a", "b"))
        val avro = SchemaBuilder.record("dummy").fields().name("e").`type`(enum).noDefault().endRecord()
        fromAvroSchema(avro) shouldBe Schema(Field("e", FieldType.String, false, Precision(0), Scale(0), false))
      }
      "convert avro int fields" {
        val avro = SchemaBuilder.record("dummy").fields().requiredInt("i").endRecord()
        fromAvroSchema(avro) shouldBe Schema(Field("i", FieldType.Int, false, Precision(0), Scale(0), false))
      }
      "convert avro long fields" {
        val avro = SchemaBuilder.record("dummy").fields().requiredLong("l").endRecord()
        fromAvroSchema(avro) shouldBe Schema(Field("l", FieldType.Long, false, Precision(0), Scale(0), false))
      }
      "convert avro string fields" {
        val avro = SchemaBuilder.record("dummy").fields().requiredString("s").endRecord()
        fromAvroSchema(avro) shouldBe Schema(Field("s", FieldType.String, false, Precision(0), Scale(0), false))
      }
    }
  }
}


