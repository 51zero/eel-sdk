package io.eels.component.hive

import io.eels._
import io.eels.component.parquet.Predicate
import io.eels.schema.{PartitionPart, Schema}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus}
import rx.lang.scala.Observable

/**
  * @param metastoreSchema  the schema as present in the metastore and used to match up with the raw data in dialects
 *                          where the schema is not present. For example with a CSV format in Hive, the metastoreSchema is required
 *                          in order to know what each column represents. We can't use the projection schema for this because the projection
 *                          schema might be in a different order.
  * @param projectionSchema the schema actually required, optional in which case the metastoreSchema will be used.
  *                         The reason the projectionSchema is pushed down to the dialects rather than being applied after is because
 *                          some file schemas can read data more efficiently if they know they can omit some fields (eg Parquet).
  * @param predicate        is pushed down to the parquet reader for efficiency
  * @param partitions       a list of partition key-values for this file. We require this to repopulate the partition
  *                         values when creating the final Row.
 */
class HiveFilePart(val dialect: HiveDialect,
                   val file: LocatedFileStatus,
                   val metastoreSchema: Schema,
                   val projectionSchema: Schema,
                   val predicate: Option[Predicate],
                   val partitions: List[PartitionPart])
                  (implicit fs: FileSystem, conf: Configuration) extends Part {

  override def data(): Observable[Row] = {
    require(projectionSchema.fieldNames.forall { it => it == it.toLowerCase() }, s"Use only lower case field names with hive")

    val map: Map[String, Any] = partitions.map { it => (it.key, it.value) }.toMap

    // the schema we send to the dialect must have any partitions removed, because those fields won't exist
    // in the data files. This is because partitions are not written and instead inferred from the hive meta store.
    val projectionWithoutPartitions = Schema(projectionSchema.fields.filterNot { it => map.contains(it.name) })
    val reader = dialect.read(file.getPath, metastoreSchema, projectionWithoutPartitions, predicate)

    // since we removed the partition fields from the target schema, we must repopulate them after the read
    reader.map { it => RowUtils.rowAlign(it, projectionSchema, map) }
  }
}