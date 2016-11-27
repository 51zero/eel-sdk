package io.eels.component.parquet

import java.util.UUID

import io.eels.schema.{DoubleType, Field, LongType, StructType}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
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

  val schema = StructType(Field("str"), Field("looong", LongType(true), true), Field("dooble", DoubleType, true))

  "ParquetReaderSupport" should {
    "support projections on doubles" in {

      val reader = ParquetReaderFn.apply(path, None, Option(schema.removeField("looong")))
      val record = reader.read()
      reader.close()

      record.get("str").toString() shouldBe "wibble"
      record.get("dooble") shouldBe 12.34
    }
    "support projections on longs" in {

      val reader = ParquetReaderFn.apply(path, None, Option(schema.removeField("str")))
      val record = reader.read()
      reader.close()

      record.get("looong") shouldBe 999L
    }
    "support full projections" in {

      val reader = ParquetReaderFn.apply(path, None, Option(schema))
      val record = reader.read()
      reader.close()

      record.get("str").toString() shouldBe "wibble"
      record.get("looong") shouldBe 999L
      record.get("dooble") shouldBe 12.34

    }
    "support non projections" in {

      val reader = ParquetReaderFn.apply(path, None, None)
      val record = reader.read()
      reader.close()

      record.get("str").toString() shouldBe "wibble"
      record.get("looong") shouldBe 999L
      record.get("dooble") shouldBe 12.34

    }
  }
}
