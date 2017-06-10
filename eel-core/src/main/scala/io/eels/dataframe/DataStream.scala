package io.eels.dataframe

import io.eels.Row
import io.eels.schema.StructType

/**
  * A DataStream is kind of like a table of data. It has fields (like columns) and rows of data. Each row
  * has an entry for each field (this may be null depending on the field definition).
  *
  * It is a lazily evaluated data structure. Each operation on a stream will create a new derived stream,
  * but those operations will only occur when a final action is performed.
  *
  * You can create a DataStream from an IO source, such as a Parquet file or a Hive table, or you may
  * create a fully evaluated one from an in memory structure. In the case of the former, the data
  * will only be loaded on demand as an action is performed.
  *
  * A DataStream is split into one or more partitions. Each partition can operate independantly
  * of the others. For example, if you filter a stream, each partition will be filtered seperately,
  * which allows it to be parallelized. If you write out a stream, each partition can be written out
  * to individual files, again allowing parallelization.
  *
  */
trait DataStream {
  outer =>

  /**
    * Returns the Schema for this stream. This call will not cause a full evaluation, but only
    * the operations required to retrieve a schema will occur. For example, on a stream backed
    * by a JDBC source, an empty resultset will be obtained in order to query the metadata for
    * the database columns.
    */
  def schema: StructType

  private[eels] def partitions(implicit em: ExecutionManager): Seq[Partition]

  /**
    * For each row in the steam, filter drop any rows which do not match the predicate.
    * EXAMPLE OF A 1-1 operation
    */
  def filter(p: (Row) => Boolean): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    // we can keep each partition as is, and just filter individually
    override def partitions(implicit em: ExecutionManager): Seq[Partition] = {
      val p1: Seq[Any] => Boolean = values => p(Row(schema, values))
      outer.partitions.map(_.filter(p1))
    }
  }

  /**
    * Returns a new DataStream where k number of rows has been dropped.
    * This operation requires a reshuffle.
    */
  def drop(k: Int): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override def partitions(implicit em: ExecutionManager): Seq[Partition] = {
      Seq(em.collapse(outer.partitions).drop(k))
    }
  }

  def withLowerCaseSchema(): DataStream = new DataStream {
    private lazy val lowerSchema = outer.schema.toLowerCase()
    override def schema: StructType = lowerSchema
    override def partitions(implicit em: ExecutionManager): Seq[Partition] = outer.partitions
  }

  /**
    * Combines two frames together such that the fields from this frame are joined with the fields
    * of the given frame. Eg, if this frame has A,B and the given frame has C,D then the result will
    * be A,B,C,D
    *
    * Each stream has different partitions so we'll need to re-partition it to ensure we have an even
    * distribution. EXAMPLE OF A RESHUFFLE.
    */
  def join(other: DataStream): DataStream = new DataStream {
    override def schema: StructType = outer.schema.join(other.schema)
    override def partitions(implicit em: ExecutionManager): Seq[Partition] = {
      // we must collapse each stream into a single partition, because otherwise each partition
      // may have differing numbers of rows and then they wouldn't match up properly
      val a = em.collapse(partitions)
      val b = em.collapse(other.partitions)
      // with two partitions we can now join together
      Seq(Partition(a.rows ++ b.rows))
    }
  }

  /**
    * Action which results in all the rows being returned in memory as a Vector.
    * Alias for 'collect()'
    */
  def toVector(implicit em: ExecutionManager): Vector[Row] = collect

  /**
    * Action which results in all the rows being returned in memory as a Vector.
    */
  def collect(implicit em: ExecutionManager): Vector[Row] = VectorAction(this).execute(em)

}

object DataStream {

  import scala.reflect.runtime.universe._

  /**
    * Create an in memory DataStream from the given Seq of Products.
    * The schema will be derived from the fields of the products using scala reflection.
    * This will result in a single partitioned DataStream.
    */
  def apply[T <: Product : TypeTag](ts: Seq[T]): DataStream = {
    val schema = StructType.from[T]
    val rows = ts.map(_.productIterator.toVector)
    apply(schema, rows)
  }

  /**
    * Create an in memory DataStream from the given Seq of values, and schema.
    * This will result in a single partitioned DataStream.
    */
  def apply(_schema: StructType, rows: Seq[Seq[Any]]): DataStream = new DataStream {
    override def schema: StructType = _schema
    override private[eels] def partitions(implicit em: ExecutionManager) = Seq(Partition(rows))
  }
}

case class Partition(rows: Seq[Seq[Any]]) {
  def filter(p: (Seq[Any]) => Boolean): Partition = Partition(rows.filter(p))
  def join(other: Partition): Partition = Partition(rows.zip(other.rows).map { case (t, u) => t ++ u })
  def drop(k: Int): Partition = Partition(rows.drop(k))
}

trait ExecutionManager {

  /**
    * Returns a new Partition which is the result of flattening all given
    * partitions into a single partition.
    */
  def collapse(partitions: Seq[Partition]): Partition

  def reshuffle(partitions: Seq[Partition], numOfPartitions: Int): Seq[Partition]
}

object ExecutionManager {
  def local = new ExecutionManager {
    override def collapse(partitions: Seq[Partition]): Partition = Partition(partitions.flatMap(_.rows))
    override def reshuffle(partitions: Seq[Partition], numOfPartitions: Int): Seq[Partition] = {
      val single = collapse(partitions).rows
      single.grouped(Math.ceil(single.size / numOfPartitions.toDouble).toInt).map { values => Partition(values) }.toSeq
    }
  }
}

trait Action[T] {
  def execute(em: ExecutionManager): T
}

case class VectorAction(ds: DataStream) extends Action[Vector[Row]] {
  def execute(em: ExecutionManager): Vector[Row] = {
    val schema = ds.schema
    ds.partitions(em).flatMap(_.rows).map(Row(schema, _)).toVector
  }
}