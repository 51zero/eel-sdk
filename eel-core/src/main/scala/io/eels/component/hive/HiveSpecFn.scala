package io.eels.component.hive

import io.eels.Column
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import scala.collection.JavaConverters._

object HiveSpecFn {
  def apply(dbName: String, tableName: String)
           (implicit fs: FileSystem, hive: HiveConf): HiveSpec = {
    val client = new HiveMetaStoreClient(hive)
    val table = client.getTable(dbName, tableName)
    val location = table.getSd.getLocation
    val tableType = table.getTableType
    val partitions = client.listPartitions(dbName, tableName, Short.MaxValue).asScala.map { partition =>
      PartitionSpec(
        partition.getValues.asScala.toList,
        partition.getSd.getLocation,
        partition.getParameters.asScala.toMap
      )
    }.toList
    val columns = table.getSd.getCols.asScala.map(HiveSchemaFns.fromHiveField).toList
    val owner = table.getOwner
    val retention = table.getRetention
    val createTime = table.getCreateTime
    val inputFormat = table.getSd.getInputFormat
    val outputFormat = table.getSd.getOutputFormat
    val serde = table.getSd.getSerdeInfo.getName
    val params = table.getParameters.asScala.toMap
    HiveSpec(dbName, tableName, location, columns, tableType, partitions, params, inputFormat, outputFormat, serde, retention, createTime, owner)
  }
}

case class HiveSpec(dbName: String,
                    tableName: String,
                    location: String,
                    columns: List[Column],
                    tableType: String,
                    partitions: List[PartitionSpec],
                    params: Map[String, String],
                    inputFormat: String,
                    outputFormat: String,
                    serde: String,
                    retention: Int,
                    createTime: Long,
                    owner: String)

case class PartitionSpec(values: List[String], location: String, params: Map[String, String])