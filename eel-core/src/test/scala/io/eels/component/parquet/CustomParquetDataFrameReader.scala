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

  val schema = io.eels.schema.StructType("id")
  val rows = Vector(io.eels.Row(schema, Vector("1")), io.eels.Row(schema, Vector("2")), io.eels.Row(schema, Vector("3")))
  DataStream.fromRows(rows).to(ParquetSink("uparquet.pq").withOverwrite(true))

  val excludes = io.eels.Row(schema, Vector("2"))
  DataStream.fromRows(excludes).to(ParquetSink("excludes_uparquet.pq").withOverwrite(true))

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

    val dataPath = new Path(parameters("path"))
    val main = ParquetSource(dataPath).toDataStream().collect
    val sparkschema = StructType(main.head.schema.fieldNames().map { name => StructField(name, StringType) })

    val excludePath = new Path("excludes_" + parameters("path"))
    val excludeIds = ParquetSource(excludePath).toDataStream().collect.map(_.values).map(_.head)

    val rows = main.filterNot { row => excludeIds.contains(row("id")) }
    new UParquet(sqlContext, sparkschema, rows)
  }
}