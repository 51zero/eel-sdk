package io.eels

import java.sql.Timestamp

import io.eels.component.parquet.{ParquetSink, ParquetSource}
import io.eels.datastream.DataStream
import io.eels.schema.{ArrayType, DecimalType, Field, IntType, Precision, Scale, StringType, StructType, TimestampMillisType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object ReadParquetEEL extends App {

  def readParquet(path: Path): Unit = {

    implicit val hadoopConfiguration = new Configuration()
    implicit val hadoopFileSystem = FileSystem.get(hadoopConfiguration)
    val rows = ParquetSource(parquetFilePath).toDataStream().collect
    rows.foreach(row => println(row))
  }

  val parquetFilePath = new Path("file:///home/sam/development/person2.parquet")
  implicit val hadoopConfiguration = new Configuration()
  implicit val hadoopFileSystem = FileSystem.get(hadoopConfiguration)

  val friendStruct = Field.createStructField("FRIEND",
    Seq(
      Field("NAME", StringType),
      Field("AGE", IntType.Signed)
    )
  )

  val personDetailsStruct = Field.createStructField("PERSON_DETAILS",
    Seq(
      Field("NAME", StringType),
      Field("AGE", IntType.Signed),
      Field("SALARY", DecimalType(Precision(38), Scale(5))),
      Field("CREATION_TIME", TimestampMillisType)
    )
  )

  val friendType = StructType(friendStruct)
  val schema = StructType(personDetailsStruct, Field("FRIENDS", ArrayType(friendType), nullable = false))

  val friends = Vector(
    Vector(Vector("John", 25)),
    Vector(Vector("Adam", 26)),
    Vector(Vector("Steven", 27))
  )

  val rows = Vector(
    Vector(Vector("Fred", 50, BigDecimal("50000.99000"), new Timestamp(System.currentTimeMillis())), friends)
  )

  try {
    DataStream.fromValues(schema, rows).to(ParquetSink(parquetFilePath).withOverwrite(true))
  } catch {
    case e: Exception => e.printStackTrace()
  }

  try {
    readParquet(parquetFilePath)
  } catch {
    case e: Exception => e.printStackTrace()
  }
}