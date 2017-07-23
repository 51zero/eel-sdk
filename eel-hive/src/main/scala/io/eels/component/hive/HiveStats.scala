package io.eels.component.hive

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import io.eels.schema.PartitionConstraint
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader

import scala.collection.JavaConverters._

trait HiveStats {

  // total number of records
  def count: Long = count(Nil)

  // total number of records in the partitions that match the constraints
  def count(constraints: Seq[PartitionConstraint]): Long

  // returns the minimum value of this field
  def min(field: String): Any = min(field, Nil)

  // returns the maximum value of this field
  def max(field: String): Any = max(field, Nil)

  // returns the minimum value of this field for the partitions that match the constraints
  def min(field: String, constraints: Seq[PartitionConstraint]): Any

  // returns the maximum value of this field for the partitions that match the constraints
  def max(field: String, constraints: Seq[PartitionConstraint]): Any
}

class ParquetHiveStats(dbName: String,
                       tableName: String,
                       table: HiveTable)
                      (implicit fs: FileSystem,
                       conf: Configuration,
                       client: IMetaStoreClient) extends HiveStats with Logging {

  private val ops = new HiveOps(client)

  private def count(path: Path) = {
    val blocks = ParquetFileReader.readFooter(fs.getConf, path, ParquetMetadataConverter.NO_FILTER).getBlocks.asScala
    blocks.map(_.getRowCount).sum
  }

  override def count(constraints: Seq[PartitionConstraint]): Long = {
    val counts = HiveTableFilesFn(dbName, tableName, table.location, constraints)
      .flatMap(_._2)
      .map(_.getPath).map(count)
    if (counts.isEmpty) 0 else counts.sum
  }

  private def minmax(field: String, constraints: Seq[PartitionConstraint]): (Any, Any) = {
    def stats[T]: (Any, Any) = {
      def min(ts: Seq[Comparable[T]]): Any = ts.reduceLeft { (a, b) => if (a.compareTo(b.asInstanceOf[T]) <= 0) a else b }
      def max(ts: Seq[Comparable[T]]): Any = ts.reduceLeft { (a, b) => if (a.compareTo(b.asInstanceOf[T]) >= 0) a else b }
      val location = new Path(ops.location(dbName, tableName))
      val (mins, maxes) = HiveTableFilesFn(dbName, tableName, location, constraints).toSeq.flatMap { case (_, files) =>
        logger.debug(s"Calculating min,max in file $files")
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

  override def min(field: String, constraints: Seq[PartitionConstraint]): Any = minmax(field, constraints)._1
  override def max(field: String, constraints: Seq[PartitionConstraint]): Any = minmax(field, constraints)._2
}