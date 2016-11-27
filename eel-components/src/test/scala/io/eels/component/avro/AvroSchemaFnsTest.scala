package io.eels.component.avro

import java.util

import io.eels.schema._
import org.apache.avro.SchemaBuilder
import org.codehaus.jackson.node.NullNode
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class AvroSchemaFnsTest extends WordSpec with Matchers {

    "toAvro" should {
      "use a union of [null, type] for a nullable column" in {
        val schema = StructType(Field("a", StringType, true))
        val fields = AvroSchemaFns.toAvroSchema(schema).getFields.asScala
        fields.head.schema().getType shouldBe org.apache.avro.Schema.Type.UNION
        fields.head.schema().getTypes.get(0).getType shouldBe org.apache.avro.Schema.Type.NULL
        fields.head.schema().getTypes.get(1).getType shouldBe org.apache.avro.Schema.Type.STRING
      }
      "set default type of NullNode for a nullable column" in {
        val schema = StructType(Field("a", StringType, true))
        val fields = AvroSchemaFns.toAvroSchema(schema).getFields
        fields.get(0).defaultValue() shouldBe NullNode.getInstance()
      }
      "not set a default value for a non null column" in {
        val schema = StructType(Field("a", IntType(true), false))
        val fields = AvroSchemaFns.toAvroSchema(schema).getFields
        (fields.get(0).defaultVal() == null) shouldBe true
        fields.get(0).schema().getType shouldBe org.apache.avro.Schema.Type.INT
      }
    }

    "fromAvroSchema" should {
      "convert avro unions [null, string] to nullable columns" in {
        val avro = SchemaBuilder.record("dummy").fields().optionalString("str").endRecord()
        AvroSchemaFns.fromAvroSchema(avro) shouldBe StructType(Field("str", StringType, true))
      }
      "convert avro unions [null, enum] to nullable columns" in {
        val enum = org.apache.avro.Schema.createEnum("myenum", null, null, util.Arrays.asList("a", "b"))
        val union = org.apache.avro.Schema.createUnion(util.Arrays.asList(enum, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL)))
        val avro = SchemaBuilder.record("dummy").fields().name("u").`type`(union).noDefault().endRecord()
        AvroSchemaFns.fromAvroSchema(avro) shouldBe StructType(Field("u", StringType, true))
      }
      "convert avro unions [null, double] to nullable double columns" in {
        val union = org.apache.avro.Schema.createUnion(util.Arrays.asList(SchemaBuilder.builder().doubleType(), SchemaBuilder.builder().nullType()))
        val avro = SchemaBuilder.record("dummy").fields().name("u").`type`(union).noDefault().endRecord()
        AvroSchemaFns.fromAvroSchema(avro) shouldBe StructType(Field("u", DoubleType, true))
      }
      "convert avro boolean fields" in {
        val avro = SchemaBuilder.record("dummy").fields().requiredBoolean("b").endRecord()
        AvroSchemaFns.fromAvroSchema(avro) shouldBe StructType(Field("b", BooleanType, false))
      }
      "convert avro float fields" in {
        val avro = SchemaBuilder.record("dummy").fields().requiredFloat("f").endRecord()
        AvroSchemaFns.fromAvroSchema(avro) shouldBe StructType(Field("f", FloatType, false))
      }
      "convert avro double fields" in {
        val avro = SchemaBuilder.record("dummy").fields().requiredDouble("d").endRecord()
        AvroSchemaFns.fromAvroSchema(avro) shouldBe StructType(Field("d", DoubleType, false))
      }
      "convert avro fixed fields to string" in {
        val fixed = org.apache.avro.Schema.createFixed("dummy", null, null, 123)
        val avro = SchemaBuilder.record("dummy").fields().name("f").`type`(fixed).noDefault().endRecord()
        AvroSchemaFns.fromAvroSchema(avro) shouldBe StructType(Field("f", StringType, false))
      }
      "convert avro enum fields to string" in {
        val enum = org.apache.avro.Schema.createEnum("myenum", null, null, util.Arrays.asList("a", "b"))
        val avro = SchemaBuilder.record("dummy").fields().name("e").`type`(enum).noDefault().endRecord()
        AvroSchemaFns.fromAvroSchema(avro) shouldBe StructType(Field("e", StringType, false))
      }
      "convert avro int fields" in {
        val avro = SchemaBuilder.record("dummy").fields().requiredInt("i").endRecord()
        AvroSchemaFns.fromAvroSchema(avro) shouldBe StructType(Field("i", IntType(true), false))
      }
      "convert avro long fields" in {
        val avro = SchemaBuilder.record("dummy").fields().requiredLong("l").endRecord()
        AvroSchemaFns.fromAvroSchema(avro) shouldBe StructType(Field("l", LongType(true), false))
      }
      "convert avro string fields" in {
        val avro = SchemaBuilder.record("dummy").fields().requiredString("s").endRecord()
        AvroSchemaFns.fromAvroSchema(avro) shouldBe StructType(Field("s", StringType, false))
    }
  }
}


