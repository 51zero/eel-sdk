package io.eels.component.hive

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.datastream.Subscriber
import io.eels.schema.StructType
import io.eels.{Part, Row}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.util.control.NonFatal

/**
  * A Hive Part that can read values from the metastore, rather than reading values from files.
  * This can be used only when the requested fields are all partition keys.
  */
class HivePartitionPart(dbName: String,
                        tableName: String,
                        projectionSchema: StructType,
                        partitionKeys: List[String], // partition keys for this table, used to map the partition values back to a map
                        dialect: HiveDialect // used to open up the files to check they exist if checkDataForPartitionOnlySources is true
                       )
                       (implicit fs: FileSystem,
                        client: IMetaStoreClient) extends Part with Logging {

  private val config = ConfigFactory.load()

  // if this is true, then we will still check that some files exist for each partition, to avoid
  // a situation where the partitions have been created in the hive metastore, but no actual
  // data has been written using those yet.
  private val partitionPartFileCheck = config.getBoolean("eel.hive.source.checkDataForPartitionOnlySources")
  logger.info(s"eel.hive.source.checkDataForPartitionOnlySources=$partitionPartFileCheck")

  // returns true if the partition exists on disk
  private def isPartitionPhysical(part: org.apache.hadoop.hive.metastore.api.Partition): Boolean = {
    val location = new Path(part.getSd.getLocation)
    logger.debug(s"Checking that partition $location has been created on disk...")
    try {
      val exists = fs.exists(location)
      if (exists) {
        logger.debug("...exists")
      } else {
        logger.debug("...not found")
      }
      exists
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Error reading $location", e)
        false
    }
  }

  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
    try {

      import scala.collection.JavaConverters._

      // each row will contain just the values from the metastore
      val rows = client.listPartitions(dbName, tableName, Short.MaxValue).asScala.filter { part =>
        !partitionPartFileCheck || isPartitionPhysical(part)
      }.map { part =>
        // the partition values are assumed to be the same order as the supplied partition keys
        // first we build a map of the keys to values, then use that map to return a Row with
        // values in the order set by the fieldNames parameter
        val map = partitionKeys.zip(part.getValues.asScala).toMap
        Row(projectionSchema, projectionSchema.fieldNames.map(map(_)).toVector)
      }

      logger.debug(s"After scanning partitions and files we have ${rows.size} rows")
      rows.iterator.grouped(10).foreach(subscriber.next)
      subscriber.completed()
    } catch {
      case t: Throwable => subscriber.error(t)
    }
  }
}
