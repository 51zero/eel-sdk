package io.eels.component.avro

import io.eels.schema.{ArrayType, Field, IntType, StructType}
import io.eels.Row
import org.apache.avro.SchemaBuilder
import org.scalatest.{Matchers, WordSpec}
import scala.collection.JavaConverters._

class AvroSerializerTest extends WordSpec with Matchers {

  private val avroSchema = SchemaBuilder.record("row").fields().requiredString("s").requiredLong("l").requiredBoolean("b").endRecord()
  private val serializer = new RecordSerializer(avroSchema)

  "AvroRecordMarshaller" should {
    "serialize from values" in {
      val record = serializer.serialize(Vector( "a", 1L, false))
      record.get("s") shouldBe "a"
      record.get("l") shouldBe 1L
      record.get("b") shouldBe false
    }
    "support rows with a different ordering to the write schema" in {
      // todo
    }
    "convert strings to longs" in {
      val record = serializer.serialize(Vector("1", "2", "true"))
      record.get("l") shouldBe 2L
    }
    "convert strings to booleans" in {
      val record = serializer.serialize(Vector("1", "2", "true"))
      record.get("b") shouldBe true
    }
    "convert longs to strings" in {
      val record = serializer.serialize(Vector(1L, "2", "true"))
      record.get("s") shouldBe "1"
    }
    "convert booleans to strings" in {
      val record = serializer.serialize(Vector(true, "2", "true"))
      record.get("s") shouldBe "true"
    }
    "support arrays" in {
      val schema = StructType(Field("a", ArrayType(IntType.Signed)))
      val serializer = new RecordSerializer(AvroSchemaFns.toAvroSchema(schema))
      val record = serializer.serialize(Vector(Array(1, 2)))
      record.get("a").asInstanceOf[java.util.List[_]].asScala.toList shouldBe List(1, 2)
    }
    "support lists" in {
      val schema = StructType(Field("a", ArrayType(IntType.Signed)))
      val serializer = new RecordSerializer(AvroSchemaFns.toAvroSchema(schema))
      val record = serializer.serialize(Vector(List(1, 2)))
      record.get("a").asInstanceOf[java.util.List[_]].asScala.toList shouldBe List(1, 2)
    }
    "support sets" in {
      val schema = StructType(Field("a", ArrayType(IntType(true))))
      val serializer = new RecordSerializer(AvroSchemaFns.toAvroSchema(schema))
      val record = serializer.serialize(Vector(Set(1, 2)))
      record.get("a").asInstanceOf[java.util.List[_]].asScala.toList shouldBe List(1, 2)
    }
    "support iterables" in {
      val schema = StructType(Field("a", ArrayType(IntType(true))))
      val serializer = new RecordSerializer(AvroSchemaFns.toAvroSchema(schema))
      val record = serializer.serialize(Vector(Iterable(1, 2)))
      record.get("a").asInstanceOf[java.util.List[_]].asScala.toList shouldBe List(1, 2)
    }
  }
}