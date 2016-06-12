package io.eels.component.hive

import io.eels.schema.Schema
import io.eels.util.Logging
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient

object HiveSchemaEvolve : Logging {

  fun apply(dbName: String, tableName: String, schema: Schema, fs: FileSystem, client: IMetaStoreClient): Unit {
    logger.debug("Checking hive:$dbName:$tableName schema for evolution")

    val ops = HiveOps(client)

    // these will be lower case
    val fields = client.getSchema(dbName, tableName).plus(ops.partitionFieldSchemas(dbName, tableName))
    val existingColumns = fields.map { it.name }
    logger.debug("hive:$dbName:$tableName fields: " + existingColumns.joinToString (","))

    val schemaColumnsLowerCase = schema.columnNames().map { it.toLowerCase() }
    logger.debug("Schema columns: " + schemaColumnsLowerCase.joinToString(","))

    // our schema can be mixed case so we must lower case to match hive
    val missingColumns = schema.fields.filterNot { existingColumns.contains(it.name.toLowerCase()) }

    if (missingColumns.isNotEmpty())
      logger.debug("Columns to be added for evolution: " + missingColumns.joinToString(","))
    else
      logger.debug("Hive schema is up to date - no evolution required")
    //missingColumns.forEach { HiveOps.addColumn(dbName, tableName, _) }
  }
}
