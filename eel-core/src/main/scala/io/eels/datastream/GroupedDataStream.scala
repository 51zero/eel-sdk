package io.eels.datastream

import com.sksamuel.exts.Logging
import io.eels.Row
import io.eels.schema.{DataType, DoubleType, Field, StructType}

object GroupedDataStream {
  val FullDatasetKeyFn: Row => Any = { _ => 0 }
}

trait GroupedDataStream {
  self =>

  // the source data stream
  def source: DataStream

  // the function that will return a key for the row
  def keyFn: Row => Any

  def aggregations: Vector[Aggregation]

  def toDataStream: DataStream = new DataStream with Logging {

    override val schema: StructType = {
      val fields = aggregations.map(agg => Field(agg.name, agg.dataType))
      StructType(
        if (keyFn == GroupedDataStream.FullDatasetKeyFn) fields
        else Field("key") +: fields
      )
    }

    //    override private[eels] def flows = {
    //
    //      val keys = new ConcurrentHashMap[Any, Boolean]()
    //
    //      // foreach forces the evaluation here
    //      try {
    //        self.source.flows.foreach { flow =>
    //          println(flow)
    //          flow.foreach { row =>
    //            println(row)
    //            val key = keyFn(row)
    //            keys.put(key, true)
    //            aggregations.foreach(_.aggregate(key, row))
    //          }
    //        }
    //      } catch {
    //        case NonFatal(e) =>
    //          logger.error("Error processing aggregation", e)
    //          e.printStackTrace()
    //      }
    //
    //      println(keys)
    //
    //      val rows = keys.keys().asScala.map { key =>
    //        val values = aggregations.map(_.value(key))
    //        Row(schema, if (keyFn == GroupedDataStream.FullDatasetKeyFn) values else key +: values)
    //      }
    //
    //      Seq(Flow(rows))
    //    }
    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = ???
  }

  def aggregation(agg: Aggregation): GroupedDataStream = new GroupedDataStream {
    override def source: DataStream = self.source
    override def aggregations: Vector[Aggregation] = self.aggregations :+ agg
    override def keyFn: (Row) => Any = self.keyFn
  }

  // actions
  def size: Long = toDataStream.size
  def collect: Vector[Row] = toDataStream.collect

  // convenience methods to add aggregations for the named fields
  def sum(field: String): GroupedDataStream = aggregation(Aggregation.sum(field))
  def count(name: String): GroupedDataStream = aggregation(Aggregation.count(name))
  def avg(name: String): GroupedDataStream = aggregation(Aggregation.avg(name))
  def min(name: String): GroupedDataStream = aggregation(Aggregation.min(name))
  def max(name: String): GroupedDataStream = aggregation(Aggregation.max(name))
}

trait Aggregation {
  def name: String // the name that will be used for the field in the output frame
  def dataType: DataType // the datatype that the schema will have for the output field
  def aggregate(key: Any, row: Row): Unit // called once per row
  def value(key: Any): Any // to retrieve the aggregated value once the aggregation has completed
}

abstract class DefaultAggregation(val name: String, val dataType: DataType) extends Aggregation

object Aggregation {

  def avg(name: String): Aggregation = new DefaultAggregation(name, DoubleType) {
    private val rows = scala.collection.mutable.Map.empty[Any, (Long, Double)]
    override def value(key: Any): Any = rows(key)._2 / rows(key)._1
    override def aggregate(key: Any, row: Row): Unit = {
      val (count, sum) = rows.getOrElseUpdate(key, (0, 0))
      rows.update(key, (count + 1, sum + row.get(name).toString.toDouble))
    }
  }

  def count(name: String): Aggregation = new DefaultAggregation(name, DoubleType) {
    private val rows = scala.collection.mutable.Map.empty[Any, Double]
    override def value(key: Any): Any = rows(key)
    override def aggregate(key: Any, row: Row): Unit = {
      val updated = rows.getOrElseUpdate(key, 0) + 1
      rows.update(key, updated)
    }
  }

  def sum(name: String) = new DefaultAggregation(name, DoubleType) {
    private val rows = scala.collection.mutable.Map.empty[Any, Double]
    override def value(key: Any): Any = rows(key)
    override def aggregate(key: Any, row: Row): Unit = {
      val updated = rows.getOrElseUpdate(key, 0D) + Option(row.get(name)).map(_.toString.toDouble).getOrElse(0D)
      rows.update(key, updated)
    }
  }

  def min(name: String) = new DefaultAggregation(name, DoubleType) {
    private val rows = scala.collection.mutable.Map.empty[Any, Double]
    override def value(key: Any): Any = rows(key)
    override def aggregate(key: Any, row: Row): Unit = {
      val updated = Math.min(
        rows.getOrElseUpdate(key, Double.MaxValue),
        Option(row.get(name)).map(_.toString.toDouble).getOrElse(Double.MaxValue)
      )
      rows.update(key, updated)
    }
  }

  def max(name: String) = new DefaultAggregation(name, DoubleType) {
    private val rows = scala.collection.mutable.Map.empty[Any, Double]
    override def value(key: Any): Any = rows(key)
    override def aggregate(key: Any, row: Row): Unit = {
      val updated = Math.max(
        rows.getOrElseUpdate(key, Double.MinValue),
        Option(row.get(name)).map(_.toString.toDouble).getOrElse(Double.MinValue)
      )
      rows.update(key, updated)
    }
  }
}