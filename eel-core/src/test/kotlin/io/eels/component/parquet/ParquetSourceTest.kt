package io.eels.component.parquet

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import java.nio.file.Paths

class ParquetSourceTest : WordSpec() {
  init {
    ParquetLogMute()

    val fs = FileSystem.get(Configuration())

    val personFile = Paths.get(javaClass.getResource("/parquetfiles/person.pq").file)
    val resourcesDir = personFile.parent

    "ParquetSource" should {
      "read schema" {
        val people = ParquetSource(personFile)
        people.schema() shouldBe Schema(
            Field("name", FieldType.String, nullable = false),
            Field("job", FieldType.String, nullable = false),
            Field("location", FieldType.String, nullable = false)
        )
      }
      "read parquet files" {
        val people = ParquetSource(personFile.toAbsolutePath()).toFrame(1).toSet().map { it.values }.toSet()
        people shouldBe setOf(
            listOf("clint eastwood", "actor", "carmel"),
            listOf("elton john", "musician", "pinner")
        )
      }
      "read multiple parquet files using file expansion" {
        val people = ParquetSource(resourcesDir.resolve("*")).toFrame(1).toSet().map { it.values }.toSet()
        people shouldBe setOf(
            listOf("clint eastwood", "actor", "carmel"),
            listOf("elton john", "musician", "pinner"),
            listOf("clint eastwood", "actor", "carmel"),
            listOf("elton john", "musician", "pinner")
        )
      }
      "merge schemas" {

        try {
          fs.delete(Path("merge1.pq"), false)
        } catch (t: Throwable) {
        }
        try {
          fs.delete(Path("merge2.pq"), false)
        } catch (t: Throwable) {
        }

        val schema1 = SchemaBuilder.builder().record("schema1").fields().requiredString("a").requiredDouble("b").endRecord()
        val schema2 = SchemaBuilder.builder().record("schema2").fields().requiredInt("a").requiredBoolean("c").endRecord()

        val writer1 = AvroParquetWriter.builder<GenericRecord>(Path("merge1.pq")).withSchema(schema1).build()
        val record1 = GenericData.Record(schema1)
        record1.put("a", "aaaaa")
        record1.put("b", 124.3)
        writer1.write(record1)
        writer1.close()

        val writer2 = AvroParquetWriter.builder<GenericRecord>(Path("merge2.pq")).withSchema(schema2).build()
        val record2 = GenericData.Record(schema2)
        record2.put("a", 111)
        record2.put("c", true)
        writer2.write(record2)
        writer2.close()

        ParquetSource(Path("merge*")).schema() shouldBe
          Schema(
              Field("a", FieldType.String, nullable = false),
              Field("b", FieldType.Double, nullable = false),
              Field("c", FieldType.Boolean, nullable = false)
          )

        fs.delete(Path(".merge1.pq.crc"), false)
        fs.delete(Path(".merge2.pq.crc"), false)
        fs.delete(Path("merge1.pq"), false)
        fs.delete(Path("merge2.pq"), false)
      }
    }
  }
}

