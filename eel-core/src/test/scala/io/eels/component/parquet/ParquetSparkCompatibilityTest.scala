package io.eels.component.parquet

import java.sql.{Date, Timestamp}

import io.eels.datastream.DataStream
import io.eels.schema._
import io.eels.{FilePattern, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, WordSpec}

case class Foo(myString: String,
               myDouble: Double,
               myLong: Long,
               myInt: Int,
               myBoolean: Boolean,
               myFloat: Float,
               myShort: Short,
               myDecimal: BigDecimal,
               myBytes: Array[Byte],
               myDate: Date,
               myTimestamp: Timestamp,
               array: Array[String],
               map: Map[String, Boolean]
              )

// tests designed to ensure that eel parquet support matches what spark generates
class ParquetSparkCompatibilityTest extends WordSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(new Configuration())

  val spark = new SparkContext(new SparkConf().setMaster("local").setAppName("parquet-spark").set("spark.driver.allowMultipleContexts", "true"))
  val session = SparkSession.builder().appName("test").master("local").getOrCreate()

  import session.sqlContext.implicits._

  val path = new Path("spark_parquet.parquet")

  fs.delete(path, true)

  // some types default to null in spark, others don't
  val schema = StructType(
    Field("myString", StringType, true),
    Field("myDouble", DoubleType, false),
    Field("myLong", LongType.Signed, false),
    Field("myInt", IntType.Signed, false),
    Field("myBoolean", BooleanType, false),
    Field("myFloat", FloatType, false),
    Field("myShort", ShortType.Signed, false),
    Field("myDecimal", DecimalType(Precision(38), Scale(18)), true),
    Field("myBytes", BinaryType, true),
    Field("myDate", DateType, true),
    Field("myTimestamp", TimestampMillisType, true),
    Field("array", ArrayType(StringType), true),
    Field("map", MapType(StringType, BooleanType), true)
  )

  // create a parquet file using spark local for all supported types and then
  // read back in using eel parquet support and compare
  "parquet reader" should {
    "read spark generated parquet files for all types" in {
      fs.delete(path, true)

      val df = session.sqlContext.createDataFrame(List(
        Foo(
          "wibble",
          13.46D,
          1414L,
          239, // integer
          true,
          1825.5F, // float
          12, // short
          72.72, // big decimal
          Array[Byte](1, 2, 3), // bytes
          new Date(79, 8, 10), // util date
          new Timestamp(1483492808000L), // sql timestamp
          Array("green", "ham", "eggs"),
          Map("a" -> true, "b" -> false)
        )
      ))

      df.write.mode(SaveMode.Overwrite).parquet(path.toString)

      val ds = ParquetSource(FilePattern(path).withFilter(path => !path.getName.contains("SUCCESS"))).toDataStream()
      ds.schema shouldBe schema

      val values = ds.collect.head
      // must convert byte array to list for deep equals
      values.update(8, values(8).asInstanceOf[Array[Byte]].toList)
      values shouldBe Vector(
        "wibble",
        13.46D,
        1414L,
        239,
        true,
        1825.5F,
        12,
        BigDecimal(72.72),
        List[Byte](1, 2, 3),
        new Date(79, 8, 10),
        new Timestamp(1483492808000L),
        Vector("green", "ham", "eggs"),
        Map("a" -> true, "b" -> false)
      )

      fs.delete(path, true)
    }

    "read arrays of structs written by eel" in {
      val eelSchema = StructType(Field("a", ArrayType(StructType("p", "q"))))
      val sparkSchema = org.apache.spark.sql.types.StructType(Seq(
        StructField("a", org.apache.spark.sql.types.ArrayType(
          org.apache.spark.sql.types.StructType(Seq(
            StructField("p", org.apache.spark.sql.types.StringType),
            StructField("q", org.apache.spark.sql.types.StringType)
          ))
        ))
      ))

      val path = new Path("spark.parquet.structs")
      DataStream.fromRows(eelSchema, Row(eelSchema, Seq(Seq(Tuple2("prince", "queen"))))).to(ParquetSink(path).withOverwrite(true))

      val df = session.sqlContext.read.parquet(path.toString)
      df.schema shouldBe sparkSchema
      df.collect.head.toSeq.head.asInstanceOf[Seq[_]].head.asInstanceOf[GenericRow].toSeq shouldBe Array("prince", "queen")

      fs.delete(path, false)
    }
  }

  "parquet writer" should {
    "generate a file compatible with spark" in {
      fs.delete(path, true)

      val row = Array(
        "flibble",
        52.972D,
        51616L,
        4536,
        true,
        2466.1F,
        55,
        BigDecimal(95.36),
        List[Byte](3, 1, 3),
        new Date(89, 8, 10),
        new Timestamp(1483492406000L),
        Vector("green", "ham", "eggs"),
        Map("a" -> true, "b" -> false)
      )

      DataStream.fromRows(schema, row).to(ParquetSink(path))

      val df = session.sqlContext.read.parquet(path.toString)
      df.schema shouldBe org.apache.spark.sql.types.StructType(
        Seq(
          StructField("myString", org.apache.spark.sql.types.StringType, true),
          StructField("myDouble", org.apache.spark.sql.types.DoubleType, true),
          StructField("myLong", org.apache.spark.sql.types.LongType, true),
          StructField("myInt", org.apache.spark.sql.types.IntegerType, true),
          StructField("myBoolean", org.apache.spark.sql.types.BooleanType, true),
          StructField("myFloat", org.apache.spark.sql.types.FloatType, true),
          StructField("myShort", org.apache.spark.sql.types.ShortType, true),
          StructField("myDecimal", org.apache.spark.sql.types.DecimalType(38, 18), true),
          StructField("myBytes", org.apache.spark.sql.types.BinaryType, true),
          StructField("myDate", org.apache.spark.sql.types.DateType, true),
          StructField("myTimestamp", org.apache.spark.sql.types.TimestampType, true),
          StructField("array", org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.StringType), true),
          StructField("map", org.apache.spark.sql.types.MapType(org.apache.spark.sql.types.StringType, org.apache.spark.sql.types.BooleanType), true)
        )
      )

      // must convert byte array to list for deep equals
      val dfvalues = df.collect().head.toSeq.toArray
      dfvalues.update(8, dfvalues(8).asInstanceOf[Array[Byte]].toList)
      // and spark will use java big decimal
      dfvalues.update(7, dfvalues(7).asInstanceOf[java.math.BigDecimal]: BigDecimal)
      dfvalues.update(11, dfvalues(11).asInstanceOf[Seq[String]].toList)
      dfvalues.toVector shouldBe row.toVector

      fs.delete(path, true)
    }

    "support nullable maps created in spark" in {
      Seq(Maps(Map("a" -> true, "b" -> false))).toDF.write.mode(SaveMode.Overwrite).save("/tmp/a")
      ParquetSource(new Path("/tmp/a")).toDataStream().collect shouldBe Seq(Vector(Map("a" -> true, "b" -> false)))
    }

    "support non-null maps created in spark" in {
      val df1 = Seq(Maps(Map("a" -> true, "b" -> false))).toDF
      val schema = org.apache.spark.sql.types.StructType(df1.schema.map(_.copy(nullable = false)))
      val df2 = df1.sqlContext.createDataFrame(df1.rdd, schema)
      df2.write.mode(SaveMode.Overwrite).save("/tmp/a")
      ParquetSource(new Path("/tmp/a")).toDataStream().collect shouldBe Seq(Vector(Map("a" -> true, "b" -> false)))
    }

    "support nullable arrays created in spark" in {

      Seq(ArrayTest(Array(1.0, 2.0, 3.0)), ArrayTest(Array(1.0, 2.0))).toDF.write.mode(SaveMode.Overwrite).save("/tmp/a")
      Seq(ArrayTest(Array(1.0, 2.0, 3.0)), ArrayTest(Array(1.0))).toDF.write.mode(SaveMode.Overwrite).save("/tmp/b")
      Seq(ArrayTest(Array(1.0, 2.0, 3.0)), ArrayTest(Array())).toDF.write.mode(SaveMode.Overwrite).save("/tmp/c")
      Seq(ArrayTest(Array(1.0, 2.0, 3.0)), ArrayTest(null)).toDF.write.mode(SaveMode.Overwrite).save("/tmp/d")

      ParquetSource(new Path("/tmp/a")).toDataStream().collect shouldBe Seq(Seq(Vector(1.0, 2.0, 3.0)), Seq(Vector(1.0, 2.0)))
      ParquetSource(new Path("/tmp/b")).toDataStream().collect shouldBe Seq(Seq(Vector(1.0, 2.0, 3.0)), Seq(Vector(1.0)))
      ParquetSource(new Path("/tmp/c")).toDataStream().collect shouldBe Seq(Seq(Vector(1.0, 2.0, 3.0)), Seq(Vector()))
      ParquetSource(new Path("/tmp/d")).toDataStream().collect shouldBe Seq(Seq(Vector(1.0, 2.0, 3.0)), Seq(null))
    }

    "support non-null arrays created in spark" in {
      val df1 = Seq(ArrayTest(Array(1.0, 2.0, 3.0)), ArrayTest(Array(1.0, 2.0))).toDF
      val schema = org.apache.spark.sql.types.StructType(df1.schema.map(_.copy(nullable = false)))
      val df2 = df1.sqlContext.createDataFrame(df1.rdd, schema)
      df2.write.mode(SaveMode.Overwrite).save("/tmp/a")

      ParquetSource(new Path("/tmp/a")).toDataStream().collect shouldBe Seq(Seq(Vector(1.0, 2.0, 3.0)), Seq(Vector(1.0, 2.0)))
    }

    "read arrays of structs written by spark" in {
      val eelSchema = StructType(Field("a", ArrayType(StructType("p", "q"))))
      val sparkSchema = org.apache.spark.sql.types.StructType(Seq(
        StructField("a", org.apache.spark.sql.types.ArrayType(
          org.apache.spark.sql.types.StructType(Seq(
            StructField("p", org.apache.spark.sql.types.StringType),
            StructField("q", org.apache.spark.sql.types.StringType)
          ))
        ))
      ))
      val df1 = Seq(Array(Tuple2("prince", "queen"))).toDF
      val df2 = session.sqlContext.createDataFrame(df1.rdd, sparkSchema)
      df2.write.mode(SaveMode.Overwrite).parquet("/tmp/structs")
      val ds = ParquetSource(FilePattern("/tmp/structs").withFilter(path => path.getName.contains(".parquet"))).toDataStream()
      ds.schema shouldBe eelSchema
      ds.head shouldBe Row(eelSchema, Seq(Seq(Seq("prince", "queen"))))
    }
  }
}

case class ArrayTest(vector: Array[Double])
case class Maps(map: Map[String, Boolean])