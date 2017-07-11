package io.eels.component.hive

import com.sksamuel.exts.io.Using
import io.eels.datastream.{Cancellable, Subscriber}
import io.eels.schema.{Partition, StructType}
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
  * @param partition        a list of partition key-values for this file. We require this to repopulate the partition
  *                         values when creating the final Row.
  */
class HiveFilePart(dialect: HiveDialect,
                   file: LocatedFileStatus,
                   metastoreSchema: StructType,
                   projectionSchema: StructType,
                   predicate: Option[Predicate],
                   partition: Partition)
                  (implicit fs: FileSystem, conf: Configuration) extends Part with Using {
  require(projectionSchema.fieldNames.forall { it => it == it.toLowerCase() }, s"Use only lower case field names with hive")

  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {

    val partitionMap: Map[String, Any] = partition.entries.map { it => (it.key, it.value) }.toMap

    // the schema we send to the dialect must have any partition fields removed, because those
    // fields won't exist in the data files. This is because partitions are not always written
    // and instead inferred from the partition itself.
    val projectionFields = projectionSchema.fields.filterNot(field => partition.containsKey(field.name))
    val projectionWithoutPartitions = StructType(projectionFields)

    // since we removed the partition fields from the target schema, we must repopulate them after the read
    // we also need to throw away the dummy field if we had an empty schema
    val publisher = dialect.input(file.getPath, metastoreSchema, projectionWithoutPartitions, predicate)
    publisher.subscribe(new Subscriber[Seq[Row]] {
      override def next(chunk: Seq[Row]): Unit = {
        val aligned = chunk.map { row =>
          if (projectionFields.isEmpty) {
            val values = projectionSchema.fieldNames().map(partitionMap.apply)
            Row(projectionSchema, values.toVector)
          } else {
            RowUtils.rowAlign(row, projectionSchema, partitionMap)
          }
        }
        subscriber.next(aligned)
      }
      override def starting(c: Cancellable): Unit = subscriber.starting(c)
      override def completed(): Unit = subscriber.completed()
      override def error(t: Throwable): Unit = subscriber.error(t)
    })
  }
}