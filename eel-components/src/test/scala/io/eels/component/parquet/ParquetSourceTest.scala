package io.eels.component.parquet

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import java.nio.file.Paths

import org.scalatest.{Matchers, WordSpec}

class ParquetSourceTest extends WordSpec with Matchers {
  ParquetLogMute()

  implicit val fs = FileSystem.get(new Configuration())

  val personFile = Paths.get(getClass.getResource("/parquetfiles/person.pq").getFile)
  val resourcesDir = personFile.getParent

  "ParquetSource" should {
    "read schema" in {
      val people = ParquetSource(personFile)
      people.schema() shouldBe Schema(
        Field("name", FieldType.String, nullable = false),
        Field("job", FieldType.String, nullable = false),
        Field("location", FieldType.String, nullable = false)
      )
    }
    "read parquet files" in {
      val people = ParquetSource(personFile.toAbsolutePath()).toFrame(1).toSet().map {
        _.values
      }
      people shouldBe Set(
        Vector("clint eastwood", "actor", "carmel"),
        Vector("elton john", "musician", "pinner")
      )
    }
    "read multiple parquet files using file expansion" in {
      val people = ParquetSource(resourcesDir.resolve("*")).toFrame(1).toSet().map {
        _.values
      }
      people shouldBe Set(
        Vector("clint eastwood", "actor", "carmel"),
        Vector("elton john", "musician", "pinner"),
        Vector("clint eastwood", "actor", "carmel"),
        Vector("elton john", "musician", "pinner")
      )
    }
    "merge schemas" in {

      try {
        fs.delete(new Path("merge1.pq"), false)
      } catch {
        case t: Throwable =>
      }
      try {
        fs.delete(new Path("merge2.pq"), false)
      } catch {
        case t: Throwable =>
      }

      val schema1 = SchemaBuilder.builder().record("schema1").fields().requiredString("a").requiredDouble("b").endRecord()
      val schema2 = SchemaBuilder.builder().record("schema2").fields().requiredInt("a").requiredBoolean("c").endRecord()

      val writer1 = AvroParquetWriter.builder[GenericRecord](new Path("merge1.pq")).withSchema(schema1).build()
      val record1 = new GenericData.Record(schema1)
      record1.put("a", "aaaaa")
      record1.put("b", 124.3)
      writer1.write(record1)
      writer1.close()

      val writer2 = AvroParquetWriter.builder[GenericRecord](new Path("merge2.pq")).withSchema(schema2).build()
      val record2 = new GenericData.Record(schema2)
      record2.put("a", 111)
      record2.put("c", true)
      writer2.write(record2)
      writer2.close()

      ParquetSource(new Path("merge*")).schema() shouldBe
        Schema(
          Field("a", FieldType.String, nullable = false),
          Field("b", FieldType.Double, nullable = false),
          Field("c", FieldType.Boolean, nullable = false)
        )

      fs.delete(new Path(".merge1.pq.crc"), false)
      fs.delete(new Path(".merge2.pq.crc"), false)
      fs.delete(new Path("merge1.pq"), false)
      fs.delete(new Path("merge2.pq"), false)
    }
  }
}

