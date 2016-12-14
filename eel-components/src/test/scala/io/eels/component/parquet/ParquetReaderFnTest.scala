package io.eels.component.parquet

import java.util.UUID

import io.eels.schema.{DoubleType, Field, LongType, StructType}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, OriginalType, PrimitiveType}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class ParquetReaderFnTest extends WordSpec with Matchers with BeforeAndAfterAll {

  val path = new Path(UUID.randomUUID().toString())

  override def afterAll(): Unit = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(path, false)
  }

  val avroSchema = SchemaBuilder.record("com.chuckle").fields()
    .requiredString("str").requiredLong("looong").requiredDouble("dooble").endRecord()

  val writer = AvroParquetWriter.builder[GenericRecord](path)
    .withSchema(avroSchema)
    .build()

  val record = new GenericData.Record(avroSchema)
  record.put("str", "wibble")
  record.put("looong", 999L)
  record.put("dooble", 12.34)
  writer.write(record)
  writer.close()

  val schema = StructType(Field("str", nullable = false), Field("looong", LongType(true), nullable = false), Field("dooble", DoubleType, nullable = false))

  "ParquetReaderFn" should {
    "read schema" in {
      val reader = ParquetReaderFn(path, None, Option(ParquetSchemaFns.toParquetSchema(schema.removeField("looong"), name = "com.chuckle")))
      val group = reader.read()
      reader.close()

      group.getType shouldBe new MessageType("com.chuckle",
        new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "str", OriginalType.UTF8),
        new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "dooble")
      )
    }
    "support projections on doubles" in {

      val reader = ParquetReaderFn(path, None, Option(ParquetSchemaFns.toParquetSchema(schema.removeField("looong"))))
      val group = reader.read()
      reader.close()

      group.getString("str", 0) shouldBe "wibble"
      group.getDouble("dooble", 0) shouldBe 12.34
    }
    "support projections on longs" in {

      val reader = ParquetReaderFn(path, None, Option(ParquetSchemaFns.toParquetSchema(schema.removeField("str"))))
      val group = reader.read()
      reader.close()

      group.getLong("looong", 0) shouldBe 999L
    }
    "support full projections" in {

      val reader = ParquetReaderFn(path, None, Option(ParquetSchemaFns.toParquetSchema(schema)))
      val group = reader.read()
      reader.close()

      group.getString("str", 0) shouldBe "wibble"
      group.getLong("looong", 0) shouldBe 999L
      group.getDouble("dooble", 0) shouldBe 12.34

    }
    "support non projections" in {

      val reader = ParquetReaderFn(path, None, None)
      val group = reader.read()
      reader.close()

      group.getString("str", 0) shouldBe "wibble"
      group.getLong("looong", 0) shouldBe 999L
      group.getDouble("dooble", 0) shouldBe 12.34

    }
  }
}
