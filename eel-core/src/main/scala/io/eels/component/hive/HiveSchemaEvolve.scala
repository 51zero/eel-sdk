package io.eels.component.hive

import com.sksamuel.scalax.Logging
import io.eels.Schema
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.collection.JavaConverters._

object HiveSchemaEvolve extends Logging {

  def apply(dbName: String, tableName: String, schema: Schema)
           (implicit fs: FileSystem, hiveConf: HiveConf, client: IMetaStoreClient): Unit = {
    logger.debug(s"Checking hive:$dbName:$tableName schema for evolution")

    // these will be lower case
    val fields = client.getSchema(dbName, tableName).asScala ++ HiveOps.partitionKeys(dbName, tableName)
    val existingColumns = fields.map(_.getName)
    logger.debug(s"hive:$dbName:$tableName fields: " + existingColumns.mkString(","))

    val schemaColumnsLowerCase = schema.columnNames.map(_.toLowerCase)
    logger.debug("Schema columns: " + schemaColumnsLowerCase.mkString(","))

    // our schema can be mixed case so we must lower case to match hive
    val missingColumns = schema.columns.filterNot(existingColumns contains _.name.toLowerCase)

    if (missingColumns.nonEmpty)
      logger.debug("Columns to be added for evolution: " + missingColumns.mkString(","))
    else
      logger.debug("Hive schema is up to date - no evolution required")
    missingColumns.foreach(HiveOps.addColumn(dbName, tableName, _))
  }
}
