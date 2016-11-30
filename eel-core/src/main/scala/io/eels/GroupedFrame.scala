package io.eels

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.function.Consumer

import com.sksamuel.exts.Logging
import io.eels.schema.{DataType, DoubleType, Field, StructType}
import reactor.core.publisher.Flux

import scala.collection.JavaConverters._

object GroupedFrame {
  val FullDatasetKeyFn: Row => Any = { row => 0 }
}

trait GroupedFrame {
  outer =>

  // the source frame
  def source: Frame

  // the function that will return a key for the row
  def keyFn: Row => Any

  def aggregations: Vector[Aggregation]

  def toFrame(): Frame = new Frame with Logging {

    override val schema: StructType = {
      val fields = aggregations.map(agg => Field(agg.name, agg.dataType))
      StructType(
        if (keyFn == GroupedFrame.FullDatasetKeyFn) fields
        else Field("key") +: fields
      )
    }

    override def rows(): Flux[Row] = {

      val keys = scala.collection.mutable.Set.empty[Any]
      val latch = new CountDownLatch(1)

      source.rows.subscribe(new Consumer[Row] {
        override def accept(row: Row): Unit = {
          val key = keyFn(row)
          keys.add(key)
          aggregations.foreach(_.aggregate(key, row))
        }
      }, new Consumer[Throwable] {
        override def accept(t: Throwable): Unit = {
          logger.error("Error when building grouped result", t)
          latch.countDown()
        }
      }, new Runnable {
        override def run(): Unit = {
          logger.debug("Subscription on grouped data has completed")
          latch.countDown()
        }
      })

      latch.await(100, TimeUnit.DAYS)

      val rows = keys.map { key =>
        val values = aggregations.map(_.value(key))
        Row(schema, if (keyFn == GroupedFrame.FullDatasetKeyFn) values else key +: values)
      }

      Flux.fromIterable(rows.asJava)
    }
  }

  def aggregation(agg: Aggregation): GroupedFrame = new GroupedFrame {
    override def source = outer.source
    override def aggregations: Vector[Aggregation] = outer.aggregations :+ agg
    override def keyFn: (Row) => Any = outer.keyFn
  }

  // actions
  def size(): Long = toFrame().size()
  def collect(): Vector[Row] = toFrame().collect()

  // convenience methods to add aggregations for the named fields
  def sum(field: String): GroupedFrame = aggregation(Aggregation.sum(field))
  def count(name: String): GroupedFrame = aggregation(Aggregation.count(name))
  def avg(name: String): GroupedFrame = aggregation(Aggregation.avg(name))
  def min(name: String): GroupedFrame = aggregation(Aggregation.min(name))
  def max(name: String): GroupedFrame = aggregation(Aggregation.max(name))
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
      val updated = rows.getOrElseUpdate(key, 0) + row.get(name).toString.toDouble
      rows.update(key, updated)
    }
  }

  def min(name: String) = new DefaultAggregation(name, DoubleType) {
    private val rows = scala.collection.mutable.Map.empty[Any, Double]
    override def value(key: Any): Any = rows(key)
    override def aggregate(key: Any, row: Row): Unit = {
      val updated = Math.min(rows.getOrElseUpdate(key, Double.MaxValue), row.get(name).toString.toDouble)
      rows.update(key, updated)
    }
  }

  def max(name: String) = new DefaultAggregation(name, DoubleType) {
    private val rows = scala.collection.mutable.Map.empty[Any, Double]
    override def value(key: Any): Any = rows(key)
    override def aggregate(key: Any, row: Row): Unit = {
      val updated = Math.max(rows.getOrElseUpdate(key, Double.MinValue), row.get(name).toString.toDouble)
      rows.update(key, updated)
    }
  }
}