package io.eels.component.hive

import io.eels.Partition
import io.eels.Row
import io.eels.Schema
import io.eels.component.Part
import io.eels.component.Predicate
import io.eels.component.SourceReader
import io.eels.map
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus

class HiveFilePart(val dialect: HiveDialect,
                   val file: LocatedFileStatus,
                   val partition: Partition,
                   val metastoreSchema: Schema,
                   val schema: Schema,
                   val predicate: Predicate?,
                   val partitionKeys: List<String>,
                   val fs: FileSystem) : Part {

  override fun reader(): SourceReader {

    // the schema we send to the reader must have any partitions removed, because those columns won't exist
    // in the data files. This is because partitions are not written and instead inferred from the hive meta store.
    val dataSchema = Schema(schema.columns.filterNot { partitionKeys.contains(it.name) })
    val reader = dialect.reader(file.path, metastoreSchema, dataSchema, predicate, fs)

    return object : SourceReader {
      override fun close(): Unit = reader.close()

      // when we read a row back from the dialect reader, we must repopulate any partition columns requested,
      // because those values are not stored in hive, but inferred from the meta store
      override fun iterator(): Iterator<Row> = reader.iterator().map { row ->
        schema.columnNames().map {
          // todo add in partition columns
          // map.getOrElse(it, partition.get(columnName).orNull)
          row
        }
        row
      }
    }
  }
}