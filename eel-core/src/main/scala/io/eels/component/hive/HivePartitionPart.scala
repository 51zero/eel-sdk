package io.eels.component.hive

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.util.Logging
import io.eels.{Part, Predicate, Row}
import io.eels.schema.Schema
import io.eels.component.Part
import io.eels.component.Predicate
import io.eels.util.Option
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import rx.lang.scala.Observable

import scala.reflect.io.Path

/**
 * A Hive Part that can read values from the metastore, rather than reading values from files.
 * This can be used only when the requested fields are all partition partitionKeys.
 */
class HivePartitionPart(val dbName: String,
                        val tableName: String,
                        val fieldNames: List[String],
                        val partitionKeys: List[String],
                        val metastoreSchema: Schema,
                        val predicate: Option[Predicate],
                        val dialect: HiveDialect,
                        val fs: FileSystem,
                        val client: IMetaStoreClient) extends Part with Logging {

  private val config = ConfigFactory.load()

  // if this is true, then we will still check that some files exist for each partition, to avoid
  // a situation where the partitions have been created in the hive metastore, but no actual
  // data has been written using those yet.
  private val partitionPartFileCheck: Boolean =
  config.getBoolean("eel.hive.source.partitionPartFileCheck")
  logger.info(s"eel.hive.source.partitionPartFileCheck=$partitionPartFileCheck")

  override def data(): Observable[Row] = {
    // the schema we use for the parquet reader must have at least one field specified, so we can
    // just use the full metastore schema
    val values = client.listPartitions(dbName, tableName, Short.MaxValue).filter {
      !partitionPartFileCheck || new HiveFileScanner(Path(it.sd.location), fs).any {
        try {
          // val reader = dialect.reader(it.path, metastoreSchema, metastoreSchema, predicate, fs)
          // todo fix this
          true
        } catch(t: Throwable) {
          logger.warn(s"Error reading ${it.path} to check for data")
          false
        }
      }
    }

//        .map {
//      val map = partitionKeys.zip(it.getValues.asScala).toMap
//      fieldNames.map(map(_)).toVector
//    }

    throw new UnsupportedOperationException()
  }
}
