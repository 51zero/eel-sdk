package io.eels.component.hive

import com.typesafe.config.ConfigFactory
import io.eels.util.Logging
import io.eels.Row
import io.eels.schema.Schema
import io.eels.component.Part
import io.eels.component.Predicate
import io.eels.util.Option
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import rx.Observable

/**
 * A Hive Part that can read values from the metastore, rather than reading values from files.
 * This can be used only when the requested fields are all partition keys.
 */
class HivePartitionPart(val dbName: String,
                        val tableName: String,
                        val fieldNames: List<String>,
                        val partitionKeys: List<String>,
                        val metastoreSchema: Schema,
                        val predicate: Option<Predicate>,
                        val dialect: HiveDialect,
                        val fs: FileSystem,
                        val client: IMetaStoreClient) : Part, Logging {

  private val config = ConfigFactory.load()

  // if this is true, then we will still check that some files exist for each partition, to avoid
  // a situation where the partitions have been created in the hive metastore, but no actual
  // data has been written using those yet.
  private val partitionPartFileCheck: Boolean =
      config.getBoolean("eel.hive.source.partitionPartFileCheck").apply {
        logger.info("eel.hive.source.partitionPartFileCheck=$this")
      }

  override fun data(): Observable<Row> {
    // the schema we use for the parquet reader must have at least one field specified, so we can
    // just use the full metastore schema
    val values = client.listPartitions(dbName, tableName, Short.MAX_VALUE).filter {
      !partitionPartFileCheck || HiveFileScanner(Path(it.sd.location), fs).any {
        try {
          val reader = dialect.reader(it.path, metastoreSchema, metastoreSchema, predicate, fs)
          // todo fix this
          true
        } catch(t: Throwable) {
          logger.warn("Error reading ${it.path} to check for data")
          false
        }
      }
    }

//        .map {
//      val map = partitionKeys.zip(it.getValues.asScala).toMap
//      columnNames.map(map(_)).toVector
//    }

    throw UnsupportedOperationException()
  }
}
