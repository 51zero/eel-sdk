package io.eels.component.hive

import io.eels.schema._
import org.scalatest.{FunSuite, Matchers}

class HiveDDLTest extends FunSuite with Matchers {

  val fields = Seq(Field("str", StringType), Field("i", BigIntType), Field("b", BooleanType))

  test("generate valid statement") {
    HiveDDL.showDDL("MYTAB", fields) shouldBe
      """CREATE TABLE IF NOT EXISTS `MYTAB` (
        |   `str` string,
        |   `i` bigint,
        |   `b` boolean)
        |ROW FORMAT SERDE
        |   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS INPUTFORMAT
        |   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |OUTPUTFORMAT
        |   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'""".stripMargin
  }

  test("include partitions") {
    HiveDDL.showDDL("MYTAB", fields, partitions = List("a", "b", "c")) shouldBe
      """CREATE TABLE IF NOT EXISTS `MYTAB` (
        |   `str` string,
        |   `i` bigint,
        |   `b` boolean)
        |PARTITIONED BY (
        |   `a` string,
        |   `b` string,
        |   `c` string)
        |ROW FORMAT SERDE
        |   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS INPUTFORMAT
        |   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |OUTPUTFORMAT
        |   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'""".stripMargin
  }

  test("include location if set") {
    HiveDDL.showDDL("MYTAB", fields, location = Some("hdfs://location")) shouldBe
      """CREATE TABLE IF NOT EXISTS `MYTAB` (
        |   `str` string,
        |   `i` bigint,
        |   `b` boolean)
        |LOCATION 'hdfs://location'
        |ROW FORMAT SERDE
        |   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS INPUTFORMAT
        |   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |OUTPUTFORMAT
        |   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'""".stripMargin
  }

  test("include inputFormat") {
    HiveDDL.showDDL("MYTAB", fields, inputFormat = "a.b.c") shouldBe
      """CREATE TABLE IF NOT EXISTS `MYTAB` (
        |   `str` string,
        |   `i` bigint,
        |   `b` boolean)
        |ROW FORMAT SERDE
        |   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS INPUTFORMAT
        |   'a.b.c'
        |OUTPUTFORMAT
        |   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'""".stripMargin
  }

  test("include outputFormat") {
    HiveDDL.showDDL("MYTAB", fields, outputFormat = "a.b.c") shouldBe
      """CREATE TABLE IF NOT EXISTS `MYTAB` (
        |   `str` string,
        |   `i` bigint,
        |   `b` boolean)
        |ROW FORMAT SERDE
        |   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS INPUTFORMAT
        |   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |OUTPUTFORMAT
        |   'a.b.c'""".stripMargin
  }

  test("include TBLPROPERTIES if set") {
    HiveDDL.showDDL("MYTAB", fields, props = Map("a" -> "b", "c" -> "d")) shouldBe
      """CREATE TABLE IF NOT EXISTS `MYTAB` (
        |   `str` string,
        |   `i` bigint,
        |   `b` boolean)
        |ROW FORMAT SERDE
        |   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        |STORED AS INPUTFORMAT
        |   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        |OUTPUTFORMAT
        |   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        |TBLPROPERTIES ('a'='b','c'='d')""".stripMargin
  }

  test("should allow implict from schema") {
    val schema = StructType(fields)
    import HiveDDL._
    schema.showDDL("mytab", format = HiveFormat.Parquet) shouldBe
      "CREATE TABLE IF NOT EXISTS `mytab` (\n   `str` string,\n   `i` bigint,\n   `b` boolean)\nROW FORMAT SERDE\n   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\nSTORED AS INPUTFORMAT\n   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\nOUTPUTFORMAT\n   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
  }
}
