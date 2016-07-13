package io.eels.component.hive

import io.eels.Row
import io.eels.component.Part
import io.eels.component.Predicate
import io.eels.schema.Schema
import io.eels.util.Option
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import rx.Observable

/**
 * @metastoreSchema the schema as present in the metastore and used to match up with the raw data in dialects
 * where the schema is not present. For example with a CSV format in Hive, the metastoreSchema is required
 * in order to know what each column represents. We can't use the projection schema for this because the projection
 * schema might be in a different order.
 *
 * @projectionSchema the schema actually required, optional in which case the metastoreSchema will be used.
 * The reason the projectionSchema is pushed down to the dialects rather than being applied after is because
 * some file schemas can read data more efficiently if they know they can omit some fields (eg Parquet).
 *
 * @predicate is pushed down to the parquet reader for efficiency
 *
 * @partitions a list of partition key-values for this file. We require this to repopulate the partition
 * values when creating the final Row.
 */
class HiveFilePart(val dialect: HiveDialect,
                   val file: LocatedFileStatus,
                   val metastoreSchema: Schema,
                   val projectionSchema: Schema,
                   val predicate: Option<Predicate>,
                   val partitions: List<PartitionPart>,
                   val fs: FileSystem) : Part {

  override fun data(): Observable<Row> {
    require(projectionSchema.fieldNames().all { it == it.toLowerCase() }, { "B Use only lower case field names with hive" })

    val map = partitions.map { it.key to it.value }.toMap()

    // the schema we send to the dialect must have any partitions removed, because those fields won't exist
    // in the data files. This is because partitions are not written and instead inferred from the hive meta store.
    val projectionWithoutPartitions = Schema(projectionSchema.fields.filterNot { map.containsKey(it.name) })
    val reader = dialect.read(file.path, metastoreSchema, projectionWithoutPartitions, predicate, fs)

    // since we removed the partition fields from the target schema, we must repopulate them after the read
    return reader.map { rowRepopulator(it, projectionSchema, map) }
  }
}

/**
 * Accepts a Row and reformats it according to the target schema, using the lookup map for the missing values.
 */
fun rowRepopulator(row: Row, targetSchema: Schema, lookup: Map<String, Any> = emptyMap()): Row {
  val values = targetSchema.fieldNames().map { fieldName ->
    when {
      lookup.containsKey(fieldName) -> lookup[fieldName]
      else -> row.get(fieldName)
    }
  }
  return Row(targetSchema, values)
}