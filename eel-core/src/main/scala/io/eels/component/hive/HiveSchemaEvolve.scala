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
    val fieldNames = fields.map(_.getName)
    val required = schema.columns.filterNot(fieldNames contains _.name)
    if (required.nonEmpty)
      logger.debug("Columns to be added for evolution=" + required.mkString(","))
    else
      logger.debug("Hive schema is up to date - no evolution required")
    required.foreach(HiveOps.addColumn(dbName, tableName, _))
  }
}
