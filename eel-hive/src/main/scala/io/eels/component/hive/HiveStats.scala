package io.eels.component.hive

import com.sksamuel.exts.OptionImplicits._
import io.eels.schema.{Partition, PartitionConstraint}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader

import scala.collection.JavaConverters._

trait HiveStats {

  def dbName: String

  def tableName: String

  // total number of records
  def count: Long

  // total number of records in the partitions that match the constraints
  def count(constraints: Seq[PartitionConstraint]): Long

  // returns the record count for a particular partition
  def count(partition: Partition): Long

  // returns the minimum value of this field in the partitions that match the constraints
  def min(field: String): Any

  // returns the maximum value of this field in the partitions that match the constraints
  def max(field: String): Any

  // returns the minimum value of this field for a particular partition
  def min(field: String, partition: Partition): Any

  // returns the maximum value of this field for a particular partition
  def max(field: String, partition: Partition): Any
}

class ParquetHiveStats(override val dbName: String,
                       override val tableName: String,
                       table: HiveTable)
                      (implicit fs: FileSystem,
                       conf: Configuration,
                       client: IMetaStoreClient) extends HiveStats {

  private val ops = new HiveOps(client)
  val dialect = table.dialect

  private def count(path: Path) = {
    val blocks = ParquetFileReader.readFooter(fs.getConf, path, ParquetMetadataConverter.NO_FILTER).getBlocks.asScala
    blocks.map(_.getRowCount).sum
  }

  def count: Long = {
    val fileCounts = if (!table.hasPartitions) {
      HiveFileScanner(table.location, false).map(_.getPath).map(count)
    } else {
      new HivePartitionScanner().scan(table.partitionMetaData, Nil).flatMap { case (_, files) =>
        files.map(_.getPath).map(count)
      }
    }
    if (fileCounts.isEmpty) 0 else fileCounts.sum
  }

  override def count(constraints: Seq[PartitionConstraint]): Long = {
    val fileCounts = new HivePartitionScanner().scan(table.partitionMetaData, constraints).flatMap { case (_, files) =>
      files.map(_.getPath).map(count)
    }
    if (fileCounts.isEmpty) 0 else fileCounts.sum
  }

  override def count(partition: Partition): Long = {
    table.partitionMetaData(partition) match {
      case Some(meta) =>
        val files = new HivePartitionScanner().scan(Seq(meta)).getOrElse(meta, Nil)
        val fileCounts = files.map(_.getPath).map(count)
        if (fileCounts.isEmpty) 0 else fileCounts.sum
      case None =>
        0
    }
  }

  private def minmax(field: String, constraints: Seq[PartitionConstraint]): (Any, Any) = {
    def stats[T]: (Any, Any) = {
      def min(ts: Seq[Comparable[T]]): Any = ts.reduceLeft { (a, b) => if (a.compareTo(b.asInstanceOf[T]) <= 0) a else b }
      def max(ts: Seq[Comparable[T]]): Any = ts.reduceLeft { (a, b) => if (a.compareTo(b.asInstanceOf[T]) >= 0) a else b }
      val location = new Path(ops.location(dbName, tableName))
      val (mins, maxes) = HiveTableFilesFn(dbName, tableName, location, constraints).toSeq.flatMap { case (_, files) =>
        files.flatMap { file =>
          val footer = ParquetFileReader.readFooter(conf, file, ParquetMetadataConverter.NO_FILTER)
          footer.getBlocks.asScala.map { block =>
            val column = block.getColumns.asScala.find(_.getPath.toDotString == field).getOrError(s"Unknown column $field")
            val min = column.getStatistics.genericGetMin.asInstanceOf[Comparable[T]]
            val max = column.getStatistics.genericGetMax.asInstanceOf[Comparable[T]]
            (min, max)
          }
        }
      }.unzip
      (min(mins), max(maxes))
    }
    stats[Any]
  }

  // returns the minimum value of this field in the partitions
  override def min(field: String): Any = minmax(field, Nil)._1
  // returns the maximum value of this field in the partitions
  override def max(field: String): Any = minmax(field, Nil)._2

  override def min(field: String, partition: Partition): Any = ???
  override def max(field: String, partition: Partition): Any = ???
}