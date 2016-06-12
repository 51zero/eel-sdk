package io.eels.component.hive

import io.eels.component.parquet.ParquetLogMute
import io.eels.schema.Schema
import io.eels.util.Logging
import org.apache.hadoop.hive.metastore.IMetaStoreClient

// todo rename this to something like HiveSource but not Source
class HiveProvider(val dbName: String, val tableName: String, private val client: IMetaStoreClient) : Logging {

  init {
    ParquetLogMute()
  }

  private val ops = HiveOps(client)

  // returns the full underlying schema from the metastore including partition keys
  fun schema(): Schema = ops.schema(dbName, tableName)

  /**
   * Returns all partition values for the given partition keys.
   * This operation is optimized, in that it does not need to scan files, but can retrieve the information
   * directly from the hive metastore.
   */
  //fun partitionValues(keys: List<String>): List<List<String>> = ops.partitionValues(dbName, tableName, keys)(client)
}
