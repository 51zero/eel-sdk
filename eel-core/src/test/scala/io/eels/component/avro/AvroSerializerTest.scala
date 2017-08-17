package io.eels.component.avro

import io.eels.schema.{ArrayType, Field, IntType, StructType}
import io.eels.Row
import org.apache.avro.SchemaBuilder
import org.scalatest.{Matchers, WordSpec}
import scala.collection.JavaConverters._

class AvroSerializerTest extends WordSpec with Matchers {

  private val avroSchema = SchemaBuilder.record("row").fields().requiredString("s").requiredLong("l").requiredBoolean("b").endRecord()
  private val serializer = new RowSerializer(avroSchema)

  "AvroRecordMarshaller" should {
    "createReader field from values in row" in {
      val eelSchema = StructType(Field("s"), Field("l"), Field("b"))
      val record = serializer.serialize(Row(eelSchema, "a", 1L, false))
      record.get("s") shouldBe "a"
      record.get("l") shouldBe 1L
      record.get("b") shouldBe false
    }
    "only accept rows with same number of values as schema fields" in {
      intercept[IllegalArgumentException] {
        val eelSchema = StructType(Field("a"), Field("b"))
        serializer.serialize(Row(eelSchema, "a", 1L))
      }
      intercept[IllegalArgumentException] {
        val eelSchema = StructType(Field("a"), Field("b"), Field("c"), Field("d"))
        serializer.serialize(Row(eelSchema, "1", "2", "3", "4"))
      }
    }
    "support rows with a different ordering to the write schema" in {
      val eelSchema = StructType(Field("l"), Field("b"), Field("s"))
      val record = serializer.serialize(Row(eelSchema, 1L, false, "a"))
      record.get("s") shouldBe "a"
      record.get("l") shouldBe 1L
      record.get("b") shouldBe false
    }
    "convert strings to longs" in {
      val record = serializer.serialize(Row(AvroSchemaFns.fromAvroSchema(avroSchema), "1", "2", "true"))
      record.get("l") shouldBe 2L
    }
    "convert strings to booleans" in {
      val record = serializer.serialize(Row(AvroSchemaFns.fromAvroSchema(avroSchema), "1", "2", "true"))
      record.get("b") shouldBe true
    }
    "convert longs to strings" in {
      val record = serializer.serialize(Row(AvroSchemaFns.fromAvroSchema(avroSchema), 1L, "2", "true"))
      record.get("s") shouldBe "1"
    }
    "convert booleans to strings" in {
      val record = serializer.serialize(Row(AvroSchemaFns.fromAvroSchema(avroSchema), true, "2", "true"))
      record.get("s") shouldBe "true"
    }
    "support arrays" in {
      val schema = StructType(Field("a", ArrayType(IntType.Signed)))
      val serializer = new RowSerializer(AvroSchemaFns.toAvroSchema(schema))
      val record = serializer.serialize(Row(schema, Array(1, 2)))
      record.get("a").asInstanceOf[java.util.List[_]].asScala.toList shouldBe List(1, 2)
    }
    "support lists" in {
      val schema = StructType(Field("a", ArrayType(IntType.Signed)))
      val serializer = new RowSerializer(AvroSchemaFns.toAvroSchema(schema))
      val record = serializer.serialize(Row(schema, Array(1, 2)))
      record.get("a").asInstanceOf[java.util.List[_]].asScala.toList shouldBe List(1, 2)
    }
    "support sets" in {
      val schema = StructType(Field("a", ArrayType(IntType(true))))
      val serializer = new RowSerializer(AvroSchemaFns.toAvroSchema(schema))
      val record = serializer.serialize(Row(schema, Set(1, 2)))
      record.get("a").asInstanceOf[java.util.List[_]].asScala.toList shouldBe List(1, 2)
    }
    "support iterables" in {
      val schema = StructType(Field("a", ArrayType(IntType(true))))
      val serializer = new RowSerializer(AvroSchemaFns.toAvroSchema(schema))
      val record = serializer.serialize(Row(schema, Iterable(1, 2)))
      record.get("a").asInstanceOf[java.util.List[_]].asScala.toList shouldBe List(1, 2)
    }
  }
}