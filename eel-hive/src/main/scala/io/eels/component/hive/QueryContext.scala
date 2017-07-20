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
  // returns the minimum value of this field in the partitions that match the constraints
  def min(field: String): Any
  // returns the maximum value of this field in the partitions that match the constraints
  def max(field: String): Any
}

class ParquetQueryContext(dbName: String,
                          tableName: String,
                          override val constraints: Seq[PartitionConstraint])
                         (implicit fs: FileSystem,
                          conf: Configuration,
                          client: IMetaStoreClient) extends QueryContext {

  private val ops = new HiveOps(client)

  private def minmax(field: String): (Any, Any) = {
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

  // returns the minimum value of this field in the partitions that match the constraints
  override def min(field: String): Any = minmax(field)._1
  // returns the maximum value of this field in the partitions that match the constraints
  override def max(field: String): Any = minmax(field)._2
}