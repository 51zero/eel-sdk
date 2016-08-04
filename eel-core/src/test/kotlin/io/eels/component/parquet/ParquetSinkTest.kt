package io.eels.component.parquet

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

class ParquetSinkTest : WordSpec() {
  init {
    ParquetLogMute()

    val schema = Schema(
        Field("name", FieldType.String, nullable = false),
        Field("job", FieldType.String, nullable = false),
        Field("location", FieldType.String, nullable = false)
    )
    val frame = io.eels.Frame(
        schema,
        listOf("clint eastwood", "actor", "carmel"),
        listOf("elton john", "musician", "pinner")
    )

    val fs = FileSystem.get(Configuration())
    val path = Path("test.pq")

    "ParquetSink" should {
      "write schema" {
        if (fs.exists(path))
          fs.delete(path, false)
        frame.to(ParquetSink(path, fs))
        val people = ParquetSource(path)
        people.schema() shouldBe Schema(
            Field("name", FieldType.String, false),
            Field("job", FieldType.String, false),
            Field("location", FieldType.String, false)
        )
        fs.delete(path, false)
      }
      "write data" {
        if (fs.exists(path))
          fs.delete(path, false)
        frame.to(ParquetSink(path))
        ParquetSource(path).toFrame(1).toSet().map { it.values }.toSet() shouldBe
            setOf(
                listOf("clint eastwood", "actor", "carmel"),
                listOf("elton john", "musician", "pinner")
            )
        fs.delete(path, false)
      }
    }
  }
}