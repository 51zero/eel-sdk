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
    "convert avro unions [null, double] to nullable double columns" in {
      val union = org.apache.avro.Schema.createUnion(util.Arrays.asList(SchemaBuilder.builder().doubleType(), SchemaBuilder.builder().nullType()))
      val avro = SchemaBuilder.record("dummy").fields().name("u").`type`(union).noDefault().endRecord()
      AvroSchemaFns.fromAvroSchema(avro) shouldBe StructType(Field("u", DoubleType, true))
    }
  }
}


