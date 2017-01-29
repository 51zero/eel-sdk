package io.eels.component.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{FlatSpec, Matchers}


class Issue233 extends FlatSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(new Configuration())

  val session = SparkSession.builder().appName("test").master("local").getOrCreate()

  import session.sqlContext.implicits._

  val path = new Path("spark_parquet.parquet")
  fs.delete(path, false)


}