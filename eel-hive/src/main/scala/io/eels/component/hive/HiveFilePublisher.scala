package io.eels.component.hive

import com.sksamuel.exts.io.Using
import io.eels.datastream.{Subscription, Publisher, Subscriber}
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
class HiveFilePublisher(dialect: HiveDialect,
                        file: LocatedFileStatus,
                        metastoreSchema: StructType,
                        projectionSchema: StructType,
                        predicate: Option[Predicate],
                        partition: Partition)
                       (implicit fs: FileSystem, conf: Configuration) extends Publisher[Chunk] with Using {
  require(projectionSchema.fieldNames.forall { it => it == it.toLowerCase() }, s"Use only lower case field names with hive")

  override def subscribe(subscriber: Subscriber[Chunk]): Unit = {

    val partitionMap: Map[String, Any] = partition.entries.map { it => (it.key, it.value) }.toMap

    // the schema we send to the dialect must have any partition fields removed, because those
    // fields won't exist in the data files. This is because partitions are not written
    // but instead inferred from the partition string itself.
    // we will call this the read schema
    val projectionFields = projectionSchema.fields.filterNot(field => partition.containsKey(field.name))
    val readSchema = StructType(projectionFields)

    // since we removed the partition fields from the target schema, we must repopulate them after the read
    // we also need to throw away the dummy field if we had an empty schema
    val publisher = dialect.input(file.getPath, metastoreSchema, readSchema, predicate)
    publisher.subscribe(new Subscriber[Chunk] {
      override def subscribed(s: Subscription): Unit = subscriber.subscribed(s)
      override def next(chunk: Chunk): Unit = {
        val aligned: Chunk = chunk.map { row =>
          // if we had no projection fields, then that means we just had a partition only projection,
          // and so the array of values can come directly from the partition map
          if (projectionFields.isEmpty) {
            projectionSchema.fieldNames().map(partitionMap.apply).toArray
          } else {
            RowUtils.rowAlign(row, projectionSchema, readSchema, partitionMap)
          }
        }
        subscriber.next(aligned)
      }
      override def completed(): Unit = subscriber.completed()
      override def error(t: Throwable): Unit = subscriber.error(t)
    })
  }
}