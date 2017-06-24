package io.eels.component.hive

import com.sksamuel.exts.Logging
import io.eels.schema.Partition
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.collection.JavaConverters._

case class HiveTable(dbName: String, tableName: String)(implicit client: IMetaStoreClient) extends Logging {

  def truncate(): Unit = {
    new HiveOps(client).partitions(dbName, tableName).foreach(deletePartition)
  }

  def deletePartition(partition: Partition): Unit = {
    logger.debug(s"Deleting partition ${partition.pretty}")
    client.dropPartition(dbName, tableName, partition.values.asJava, true)
  }

  def drop(): Unit = {
    client.dropTable(dbName, tableName, true, true)
  }
}
