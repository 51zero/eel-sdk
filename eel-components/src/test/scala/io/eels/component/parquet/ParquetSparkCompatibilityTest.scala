package io.eels.component.parquet

import java.sql.{Date, Timestamp}

import io.eels.schema._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

case class Foo(
                myDouble: Double,
                myLong: Long,
                myInt: Int,
                myBoolean: Boolean,
                myFloat: Float,
                myShort: Short,
                myDecimal: BigDecimal,
                myBytes: Array[Byte],
                myDate: Date,
                myTimestamp: Timestamp
              )

// a suite of tests designed to ensure that eel parquet support matches the specs, and also that
// it can read/write files that other frameworks (spark, hive, impala, etc) generate.
class ParquetSparkCompatibilityTest extends FlatSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(new Configuration())

  val spark = new SparkContext(new SparkConf().setMaster("local").setAppName("sammy"))
  val session = SparkSession.builder().appName("test").master("local").getOrCreate()

  val path = new Path("spark_parquet.parquet")

  // create a parquet file using spark local for all supported types and then
  // read back in using eel parquet support and compare
  "parquet reader" should "read spark generated parquet files for all types" in {

    val df = session.sqlContext.createDataFrame(List(
      Foo(
        13.46D,
        1414L,
        239,
        true,
        1825.5F, 12, 72.72,
        Array[Byte](1, 2, 3),
        new Date(1979, 9, 10),
        new Timestamp(11112323123L)
      )
    ))

    println(df.schema)
    df.write.mode(SaveMode.Overwrite).parquet(path.toString)
    println(path)

    val frame = ParquetSource(path).toFrame()
    println(frame.schema)

    frame.schema shouldBe StructType(
      Field("myDouble", DoubleType, false),
      Field("myLong", LongType.Signed, false),
      Field("myInt", IntType.Signed, false),
      Field("myBoolean", BooleanType, false),
      Field("myFloat", FloatType, false),
      Field("myShort", ShortType.Signed, false),
      Field("myDecimal", DecimalType(Precision(38), Scale(18)), true),
      Field("myBytes", BinaryType, true),
      Field("myDate", DateType, true),
      Field("myTimestamp", TimestampType, true)
    )

    fs.delete(path, true)
  }
}


