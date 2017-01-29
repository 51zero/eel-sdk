package io.eels.component.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

case class WordVector(word: String, vector: Array[Double])

class Issue233 extends FlatSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(new Configuration())

  val session = SparkSession.builder().appName("test").master("local").getOrCreate()

  import session.sqlContext.implicits._

  val path = new Path("spark_parquet.parquet")
  fs.delete(path, false)

  "parquet" should "support nested arrays created in spark" in {

    Seq(WordVector("abc", Array(1.0, 2.0, 3.0)), WordVector("abc", Array(1.0, 2.0))).toDF.write.mode(SaveMode.Overwrite).save("/tmp/a")
    Seq(WordVector("abc", Array(1.0, 2.0, 3.0)), WordVector("abc", Array(1.0))).toDF.write.mode(SaveMode.Overwrite).save("/tmp/b")
    Seq(WordVector("abc", Array(1.0, 2.0, 3.0)), WordVector("abc", Array())).toDF.write.mode(SaveMode.Overwrite).save("/tmp/c")
    Seq(WordVector("abc", Array(1.0, 2.0, 3.0)), WordVector("abc", null)).toDF.write.mode(SaveMode.Overwrite).save("/tmp/d")

    ParquetSource(new Path("/tmp/a")).toFrame().collect().map(_.values) shouldBe Seq(Seq("abc", Vector(1.0, 2.0, 3.0)), Seq("abc", Vector(1.0, 2.0)))
    ParquetSource(new Path("/tmp/b")).toFrame().collect().map(_.values) shouldBe Seq(Seq("abc", Vector(1.0, 2.0, 3.0)), Seq("abc", Vector(1.0)))
    ParquetSource(new Path("/tmp/c")).toFrame().collect().map(_.values) shouldBe Seq(Seq("abc", Vector(1.0, 2.0, 3.0)), Seq("abc", Vector()))
    ParquetSource(new Path("/tmp/d")).toFrame().collect().map(_.values) shouldBe Seq(Seq("abc", Vector(1.0, 2.0, 3.0)), Seq("abc", null))
  }
}