package io.eels.component.hive

import io.eels.schema.{PartitionPart, StructType}
import io.eels.{Predicate, _}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus}

/**
  * @param metastoreSchema  the schema as present in the metastore and used to match up with the raw data in dialects
  *                         where the schema is not present. For example with a CSV format in Hive, the metastoreSchema is required
  *                         in order to know what each column represents. We can't use the projection schema for this because the projection
  *                         schema might be in a different order.
  * @param projectionSchema the schema actually required, optional in which case the metastoreSchema will be used.
  *                         The reason the projectionSchema is pushed down to the dialects rather than being applied after is because
  *                         some file schemas can read data more efficiently if they know they can omit some fields (eg Parquet).
  * @param predicate        predicate for filtering rows, is pushed down to the parquet reader for efficiency if
  *                         the predicate can operate on the files.
  * @param partitions       a list of partition key-values for this file. We require this to repopulate the partition
  *                         values when creating the final Row.
  */
class HiveFilePart(val dialect: HiveDialect,
                   val file: LocatedFileStatus,
                   val metastoreSchema: StructType,
                   val projectionSchema: StructType,
                   val predicate: Option[Predicate],
                   val partitions: List[PartitionPart])
                  (implicit fs: FileSystem, conf: Configuration) extends Part {
  require(projectionSchema.fieldNames.forall { it => it == it.toLowerCase() }, s"Use only lower case field names with hive")

  override def iterator2(): CloseIterator[Row] = {

    val partitionMap: Map[String, Any] = partitions.map { it => (it.key, it.value) }.toMap

    // the schema we send to the dialect must have any partitions removed, because those fields won't exist
    // in the data files. This is because partitions are not written and instead inferred from the hive meta store.
    val projectionFields = projectionSchema.fields.filterNot { it => partitionMap.contains(it.name) }

    // after removing the partitions, we might have an empty projection, which won't work
    // in parquet, so we can ask for the simplest projection which is just a single field (which we'll then throw
    // away once the results come back and replace with the partition values)
    val projectionWithoutPartitions = {
      if (projectionFields.isEmpty)
        StructType(metastoreSchema.fields.head)
      else
        StructType(projectionFields)
    }

    val CloseIterator(closeable, iterator) = dialect.read2(file.getPath, metastoreSchema, projectionWithoutPartitions, predicate)

    // since we removed the partition fields from the target schema, we must repopulate them after the read
    // we also need to throw away the dummy field if we had an empty schema
    val mapped = iterator.map { row =>
      if (projectionFields.isEmpty) {
        val values = projectionSchema.fieldNames().map(partitionMap.apply)
        Row(projectionSchema, values.toVector)
      } else {
        RowUtils.rowAlign(row, projectionSchema, partitionMap)
      }
    }

    CloseIterator(closeable, mapped)
  }
}