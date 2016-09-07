package io.eels.component.hive

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.component.parquet.Predicate
import io.eels.schema.Schema
import io.eels.{Part, Row}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import rx.lang.scala.Observable

/**
 * A Hive Part that can read values from the metastore, rather than reading values from files.
 * This can be used only when the requested fields are all partition partitionKeys.
 */
class HivePartitionPart(dbName: String,
                        tableName: String,
                        fieldNames: List[String],
                        partitionKeys: List[String],
                        metastoreSchema: Schema,
                        predicate: Option[Predicate],
                        dialect: HiveDialect)
                       (implicit fs: FileSystem,
                        client: IMetaStoreClient) extends Part with Logging {

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
    import scala.collection.JavaConverters._
    val values = client.listPartitions(dbName, tableName, Short.MaxValue).asScala.filter { it =>
      !partitionPartFileCheck || HiveFileScanner(new Path(it.getSd.getLocation)).exists { it =>
        try {
          // val reader = dialect.reader(it.path, metastoreSchema, metastoreSchema, predicate, fs)
          // todo fix this
          true
        } catch {
          case t: Throwable =>
          logger.warn(s"Error reading ${it.getPath} to check for data")
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
