package io.eels.spark

import io.eels.schema.{DecimalType, Field, LongType, Precision, Scale, StringType, StructType}
import org.apache.spark.sql.types.StructField
import org.scalatest.{FunSuite, Matchers}

class SparkSchemaFnsTest extends FunSuite with Matchers {

  test("eel schema to spark") {
    val schema = StructType(Field("a", StringType), Field("b", DecimalType(Precision(14), Scale(4))), Field("c", LongType.Signed))
    SparkSchemaFns.toSparkSchema(schema) shouldBe
      org.apache.spark.sql.types.StructType(
        Seq(
          StructField("a", org.apache.spark.sql.types.StringType, true),
          StructField("b", org.apache.spark.sql.types.DecimalType(14, 4), true),
          StructField("c", org.apache.spark.sql.types.LongType, true)
        )
      )
  }

  test("spark schema to eel") {
    val schema = org.apache.spark.sql.types.StructType(
      Seq(
        StructField("a", org.apache.spark.sql.types.StringType, true),
        StructField("b", org.apache.spark.sql.types.DecimalType(14, 4), true),
        StructField("c", org.apache.spark.sql.types.LongType, true)
      )
    )
    SparkSchemaFns.fromSparkSchema(schema) shouldBe
      StructType(Field("a", StringType), Field("b", DecimalType(Precision(14), Scale(4))), Field("c", LongType.Signed))
  }
}
