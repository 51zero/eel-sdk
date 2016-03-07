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

    val fields = client.getSchema(dbName, tableName).asScala
    val partitionKeys = HiveOps.partitionKeys(dbName, tableName)
    val existingColumns = (fields ++ partitionKeys).map(_.getName)
    logger.debug(s"hive:$dbName:$tableName contains these columns: " + existingColumns.mkString(","))

    val missingColumns = schema.columns.filterNot(existingColumns contains _.name)
    if (missingColumns.nonEmpty)
      logger.debug("Columns to be added for evolution: " + missingColumns.mkString(","))
    else
      logger.debug("Hive schema is up to date - no evolution required")
    missingColumns.foreach(HiveOps.addColumn(dbName, tableName, _))
  }
}
