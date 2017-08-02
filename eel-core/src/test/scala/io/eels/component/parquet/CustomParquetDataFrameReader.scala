package io.eels.component.parquet

import com.sksamuel.exts.Logging
import io.eels.datastream.DataStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, PrunedScan, RelationProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object CustomParquetDataFrameReader extends App {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.get(conf)

  val row = io.eels.Row(io.eels.schema.StructType("a", "b"), Vector("1", "2"))
  val ds = DataStream.fromRows(row)
  ds.to(ParquetSink("uparquet.pq").withOverwrite(true))

  val spark = new SparkContext(new SparkConf().setMaster("local").setAppName("test").set("spark.driver.allowMultipleContexts", "true"))
  val session = SparkSession.builder().appName("test").master("local").getOrCreate()

  val df = session.sqlContext.read.format("io.eels.component.parquet.UParquetDataSource").load("uparquet.pq")
  println(df.collect.toList)
}

class UParquetRow(values:Seq[Any]) extends Row {
  override def length: Int = values.length
  override def get(i: Int): Any = values(i)
  override def copy(): Row = new UParquetRow(values)
}

class UParquet(override val sqlContext: SQLContext,
               override val schema: StructType,
               rows: Vector[io.eels.Row]) extends BaseRelation with PrunedScan with Logging {
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val sparkrows = rows.map(_.values).map(new UParquetRow(_))
    sqlContext.sparkContext.parallelize(sparkrows)
  }
}

class UParquetDataSource extends DataSourceRegister with RelationProvider with Logging {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.get(conf)

  override def shortName(): String = "uparquet"
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    logger.info(s"Opening u parquet file $parameters")
    val path = new Path(parameters("path"))
    val ds = ParquetSource(path).toDataStream()
    val sparkschema = StructType(ds.schema.fieldNames().map { name => StructField(name, StringType) })
    new UParquet(sqlContext, sparkschema, ds.collect)
  }
}