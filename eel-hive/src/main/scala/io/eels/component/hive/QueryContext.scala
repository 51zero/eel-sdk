package io.eels.component.hive

import io.eels.schema.PartitionConstraint
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import scala.collection.JavaConverters._
import com.sksamuel.exts.OptionImplicits._

trait QueryContext {
  def constraints: Seq[PartitionConstraint]
  // returns the maximum value of this field in the partitions that match the constraints
  def maxLong(field: String): Long
  // returns the minimum value of this field in the partitions that match the constraints
  def minLong(field: String): Long
}

class ParquetQueryContext(dbName: String,
                          tableName: String,
                          override val constraints: Seq[PartitionConstraint])
                         (implicit fs: FileSystem,
                          conf: Configuration,
                          client: IMetaStoreClient) extends QueryContext {

  private val ops = new HiveOps(client)

  private def minmax(field: String): Seq[(Any, Any)] = {
    val location = new Path(ops.location(dbName, tableName))
    HiveTableFilesFn(dbName, tableName, location, constraints).flatMap { case (partition, files) =>
      files.flatMap { file =>
        val footer = ParquetFileReader.readFooter(conf, file, ParquetMetadataConverter.NO_FILTER)
        footer.getBlocks.asScala.map { block =>
          val column = block.getColumns.asScala.find(_.getPath.toDotString == field).getOrError(s"Unknown column $field")
          (column.getStatistics.genericGetMin, column.getStatistics.genericGetMax)
        }
      }
    }.toSeq
  }

  // returns the maximum value of this field in the partitions that match the constraints
  override def maxLong(field: String): Long = minmax(field).unzip._2.map(_.toString.toLong).max

  // returns the minimum value of this field in the partitions that match the constraints
  override def minLong(field: String): Long = minmax(field).unzip._1.map(_.toString.toLong).max
}
