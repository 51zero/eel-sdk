package io.eels.component.hive

import com.typesafe.config.ConfigFactory
import io.eels.util.Logging
import io.eels.Row
import io.eels.schema.Schema
import io.eels.component.Part
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import rx.Observable

/**
 * A Hive Part that can read values from the metastore, rather than going to the files.
 * This is used when requested columns are all partition keys.
 */
class HivePartitionPart(val dbName: String,
                        val tableName: String,
                        val columnNames: String?,
                        val partitionKeys: String?,
                        val metastoreSchema: Schema,
                        val predicate: Predicate?,
                        val dialect: HiveDialect,
                        val fs: FileSystem,
                        val client: IMetaStoreClient) : Part, Logging {

  private val config = ConfigFactory.load()

  // if this is true, then we will still check that some files exist for each partition, to avoid
  // a situation where the partitions have been created in the hive metastore, but no actual
  // data has been written using those yet.
  private val checkDataForPartitionOnlySources: Boolean by lazy {
    val b = config.getBoolean("eel.hive.source.checkDataForPartitionOnlySources")
    logger.info("Hive partition scanning checkDataForPartitionOnlySources = $b")
    b
  }

  override fun data(): Observable<Row> {
    // the schema we use for the parquet reader must have at least one column, so we can take the full metastore
    // schema and remove our partition columns
    val dataSchema = Schema(metastoreSchema.columns.filterNot { it.contains { it.name } })

    val values = client.listPartitions(dbName, tableName, Short.MAX_VALUE).filter {
      !checkDataForPartitionOnlySources || HiveFileScanner(it.sd.location, fs).any {
        try {
          val reader = dialect.reader(it.path, metastoreSchema, dataSchema, predicate)
          reader.iterator.hasNext
        } catch(t: Throwable) {
          logger.warn("Error parsing ${file.getPath} to check for data")
          false
        }
      }
//    } map { partition =>
//      val map = partitionKeys.zip(partition.getValues.asScala).toMap
//      columnNames.map(map(_)).toVector
//    }
//
//    new SourceReader {
//      override def close(): Unit = ()
//      override def iterator: Iterator[InternalRow] = values.iterator
//    }
    }


//  override fun reader(): SourceReader = {
//

//
//
//
//  }
  }
