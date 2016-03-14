package io.eels.component.hive

import io.eels._
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus}

class HiveFilePart(dialect: HiveDialect,
                   file: LocatedFileStatus,
                   partition: Partition,
                   metastoreSchema: Schema,
                   schema: Schema,
                   partitionKeys: Seq[String])
                  (implicit fs: FileSystem) extends Part {

  override def reader: SourceReader = {

    // the schema we send to the reader must have any partitions removed, because those columns won't exist
    // in the data files. This is because partitions are not written and instead inferred from the hive meta store.
    val dataSchema = Schema(schema.columns.filterNot(partitionKeys contains _.name))
    val reader = dialect.reader(file.getPath, metastoreSchema, dataSchema)

    new SourceReader {
      override def close(): Unit = reader.close()

      // when we read a row back from the dialect reader, we must repopulate any partition columns requested.
      // Again because those values are not stored in hive, but inferred from the meta store
      override def iterator: Iterator[InternalRow] = reader.iterator.map { row =>
        val map = RowUtils.toMap(dataSchema, row)
        schema.columnNames.map { columnName =>
          map.getOrElse(columnName, partition.get(columnName).orNull)
        }
      }
    }
  }
}
