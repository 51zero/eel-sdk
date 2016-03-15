package io.eels.component.parquet

import java.io.File

import io.eels.{Column, Schema, SchemaType}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.scalatest.{Matchers, WordSpec}

import scala.util.Try

class ParquetSourceTest extends WordSpec with Matchers {
  ParquetLogMute()

  implicit val fs = FileSystem.get(new Configuration)

  import scala.concurrent.ExecutionContext.Implicits.global

  val personFile = new File(getClass.getResource("/parquetfiles/person.pq").getFile)
  val resourcesDir = personFile.getParentFile.getAbsolutePath

  "ParquetSource" should {
    "read schema" in {
      val people = ParquetSource(personFile.getAbsolutePath)
      people.schema shouldBe Schema(List(
        Column("name", SchemaType.String, false, 0, 0, true, None),
        Column("job", SchemaType.String, false, 0, 0, true, None),
        Column("location", SchemaType.String, false, 0, 0, true, None)
      ))
    }
    "read parquet files" in {
      val people = ParquetSource(personFile.getAbsolutePath).toSet.map(_.values.map(_.toString))
      people shouldBe Set(
        List("clint eastwood", "actor", "carmel"),
        List("elton john", "musician", "pinner")
      )
    }
    "read multiple parquet files using file expansion" in {
      val people = ParquetSource(resourcesDir + "/*").toSet.map(_.values.map(_.toString))
      people shouldBe Set(
        List("clint eastwood", "actor", "carmel"),
        List("elton john", "musician", "pinner"),
        List("clint eastwood", "actor", "carmel"),
        List("elton john", "musician", "pinner")
      )
    }
    "merge schemas" in {

      Try {
        fs.delete(new Path("merge1.pq"), false)
      }
      Try {
        fs.delete(new Path("merge2.pq"), false)
      }

      val schema1 = SchemaBuilder.builder().record("schema1").fields().requiredString("a").requiredDouble("b").endRecord()
      val schema2 = SchemaBuilder.builder().record("schema2").fields().requiredInt("a").requiredBoolean("c").endRecord()

      val writer1 = AvroParquetWriter.builder[GenericRecord](new Path("merge1.pq")).withSchema(schema1).build()
      val record1 = new Record(schema1)
      record1.put("a", "aaaaa")
      record1.put("b", 124.3)
      writer1.write(record1)
      writer1.close()

      val writer2 = AvroParquetWriter.builder[GenericRecord](new Path("merge2.pq")).withSchema(schema2).build()
      val record2 = new Record(schema2)
      record2.put("a", 111)
      record2.put("c", true)
      writer2.write(record2)
      writer2.close()

      ParquetSource("merge*").schema shouldBe {
        Schema(List(
          Column("a", SchemaType.String, false, 0, 0, true, None),
          Column("b", SchemaType.Double, false, 0, 0, true, None),
          Column("c", SchemaType.Boolean, false, 0, 0, true, None)
        ))
      }

      Try {
        fs.delete(new Path(".merge1.pq.crc"), false)
      }
      Try {
        fs.delete(new Path(".merge2.pq.crc"), false)
      }
      Try {
        fs.delete(new Path("merge1.pq"), false)
      }
      Try {
        fs.delete(new Path("merge2.pq"), false)
      }

    }
  }
}

