package io.eels.component.hive

import com.sksamuel.scalax.Logging
import com.typesafe.config.ConfigFactory
import io.eels.{InternalRow, Part, Schema, SourceReader}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
  * Part that can read values from the metastore directly.
  * This is used when requested columns are all partition keys.
  */
class HivePartitionPart(dbName: String,
                        tableName: String,
                        columnNames: Seq[String],
                        partitionKeys: Seq[String],
                        metastoreSchema: Schema,
                        predicate: Option[Predicate],
                        dialect: HiveDialect)
                       (implicit fs: FileSystem, client: IMetaStoreClient) extends Part with Logging {

  private val config = ConfigFactory.load()
  private lazy val checkDataForPartitionOnlySources = {
    val b = config.getBoolean("eel.hive.source.checkDataForPartitionOnlySources")
    logger.info(s"Hive partition scanning checkDataForPartitionOnlySources = $b")
    b
  }

  override def reader: SourceReader = {

    // the schema we use for the parquet reader must have at least one column, so we can take the full metastore
    // schema and remove our partition columns
    val dataSchema = Schema(metastoreSchema.columns.filterNot(partitionKeys contains _.name))

    val values = client.listPartitions(dbName, tableName, Short.MaxValue).asScala.filter { partition =>
      !checkDataForPartitionOnlySources || HiveFileScanner(partition.getSd.getLocation).exists { file =>
        try {
          val reader = dialect.reader(file.getPath, metastoreSchema, dataSchema, predicate: Option[Predicate])
          reader.iterator.hasNext
        } catch {
          case NonFatal(e) => logger.warn(s"Error parsing ${file.getPath} to check for data")
            false
        }
      }
    } map { partition =>
      val map = partitionKeys.zip(partition.getValues.asScala).toMap
      columnNames.map(map(_)).toVector
    }

    new SourceReader {
      override def close(): Unit = ()
      override def iterator: Iterator[InternalRow] = values.iterator
    }
  }
}
