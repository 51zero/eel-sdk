package io.eels.component.parquet

import java.util.UUID

import io.eels.{Column, Schema, SchemaType}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class ParquetReaderSupportTest extends WordSpec with Matchers with BeforeAndAfterAll {

  val avroSchema = SchemaBuilder.record("com.chuckle").fields()
    .requiredString("str").requiredLong("looong").requiredDouble("dooble").endRecord()

  val path = new Path(UUID.randomUUID.toString)

  val writer = AvroParquetWriter.builder[GenericRecord](path)
    .withSchema(avroSchema)
    .build()

  val record = new GenericData.Record(avroSchema)
  record.put("str", "wibble")
  record.put("looong", 999l)
  record.put("dooble", 12.34)
  writer.write(record)
  writer.close()


  override protected def afterAll(): Unit = {
    val fs = FileSystem.get(new Configuration)
    fs.delete(path, false)
  }

  val schema = Schema(Column("str"), Column("looong", SchemaType.Long, true), Column("dooble", SchemaType.Double, true))

  "ParquetReaderSupport" should {
    "support projections on doubles" in {

      val reader = ParquetReaderSupport.createReader(path, Seq("str", "dooble"), schema)
      val record = reader.read()
      reader.close()

      record.get("str").toString shouldBe "wibble"
      record.get("dooble") shouldBe 12.34
    }
    "support projections on longs" in {

      val reader = ParquetReaderSupport.createReader(path, Seq("looong"), schema)
      val record = reader.read()
      reader.close()

      record.get("looong") shouldBe 999l
    }
    "support projections of all columns" in {

      val reader = ParquetReaderSupport.createReader(path, Seq("str", "looong", "dooble"), schema)
      val record = reader.read()
      reader.close()

      record.get("str").toString shouldBe "wibble"
      record.get("looong") shouldBe 999l
      record.get("dooble") shouldBe 12.34

    }
  }
}
