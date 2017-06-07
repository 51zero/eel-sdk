package io.eels.component.parquet

import java.util.UUID

import io.eels.component.avro.AvroSchemaFns
import io.eels.component.parquet.avro.AvroParquetReaderFn
import io.eels.schema.{DoubleType, Field, LongType, StructType}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class AvroParquetReaderFnTest extends WordSpec with Matchers with BeforeAndAfterAll {

  private implicit val conf = new Configuration()
  private implicit val fs = FileSystem.get(new Configuration())

  private val path = new Path(UUID.randomUUID().toString())

  override def afterAll(): Unit = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(path, false)
  }

  private val avroSchema = SchemaBuilder.record("com.chuckle").fields()
    .requiredString("str").requiredLong("looong").requiredDouble("dooble").endRecord()

  private val writer = AvroParquetWriter.builder[GenericRecord](path)
    .withSchema(avroSchema)
    .build()

  private val record = new GenericData.Record(avroSchema)
  record.put("str", "wibble")
  record.put("looong", 999L)
  record.put("dooble", 12.34)
  writer.write(record)
  writer.close()

  val schema = StructType(Field("str"), Field("looong", LongType(true), true), Field("dooble", DoubleType, true))

  "AvroParquetReaderFn" should {
    "support projections on doubles" in {

      val reader = AvroParquetReaderFn(path, None, Option(AvroSchemaFns.toAvroSchema(schema.removeField("looong"))))
      val record = reader.read()
      reader.close()

      record.get("str").asInstanceOf[Utf8].toString shouldBe "wibble"
      record.get("dooble") shouldBe 12.34
    }
    "support projections on longs" in {

      val reader = AvroParquetReaderFn(path, None, Option(AvroSchemaFns.toAvroSchema(schema.removeField("str"))))
      val record = reader.read()
      reader.close()

      record.get("looong") shouldBe 999L
    }
    "support full projections" in {

      val reader = AvroParquetReaderFn(path, None, Option(AvroSchemaFns.toAvroSchema(schema)))
      val record = reader.read()
      reader.close()

      record.get("str").asInstanceOf[Utf8].toString shouldBe "wibble"
      record.get("looong") shouldBe 999L
      record.get("dooble") shouldBe 12.34

    }
    "support non projections" in {

      val reader = AvroParquetReaderFn(path, None, None)
      val group = reader.read()
      reader.close()

      group.get("str").asInstanceOf[Utf8].toString shouldBe "wibble"
      group.get("looong") shouldBe 999L
      group.get("dooble") shouldBe 12.34

    }
  }
}
