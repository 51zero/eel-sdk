package io.eels.component.hive

import io.eels.schema.{Field, FieldType, Schema}
import org.scalatest.{FunSuite, Matchers}

class HiveDDLTest extends FunSuite with Matchers {

  val fields = Seq(Field("str", FieldType.String), Field("i", FieldType.BigInt), Field("b", FieldType.Boolean))

  test("generate valid statement") {
    HiveDDL.showDDL("MYTAB", fields) shouldBe
      "CREATE TABLE IF NOT EXISTS MYTAB\n(str string , i bigint , b boolean )\nINPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\nOUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
  }

  test("include partitions") {
    HiveDDL.showDDL("MYTAB", fields, partitions = List("a", "b", "c")) shouldBe
      "CREATE TABLE IF NOT EXISTS MYTAB\n(str string , i bigint , b boolean )\nPARTITIONED BY (a string, b string, c string)\nINPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\nOUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
  }

  test("include location if set") {
    HiveDDL.showDDL("MYTAB", fields, location = Some("hdfs://location")) shouldBe
      "CREATE TABLE IF NOT EXISTS MYTAB\n(str string , i bigint , b boolean )\nLOCATION 'hdfs://location'\nINPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\nOUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
  }

  test("include inputFormat") {
    HiveDDL.showDDL("MYTAB", fields, inputFormat = "a.b.c") shouldBe "CREATE TABLE IF NOT EXISTS MYTAB\n(str string , i bigint , b boolean )\nINPUTFORMAT 'a.b.c'\nOUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
  }

  test("include outputFormat") {
    HiveDDL.showDDL("MYTAB", fields, outputFormat = "a.b.c") shouldBe "CREATE TABLE IF NOT EXISTS MYTAB\n(str string , i bigint , b boolean )\nINPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\nOUTPUTFORMAT 'a.b.c'"
  }

  test("include TBLPROPERTIES if set") {
    HiveDDL.showDDL("MYTAB", fields, props = Map("a" -> "b", "c" -> "d")) shouldBe "CREATE TABLE IF NOT EXISTS MYTAB\n(str string , i bigint , b boolean )\nINPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\nOUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'\nTBLPROPERTIES ('a'='b','c'='d')"
  }

  test("should allow implict from schema") {
    val schema = Schema(fields)
    import HiveDDL._
    schema.showDDL("mytab") shouldBe
      "CREATE TABLE IF NOT EXISTS mytab\n(str string , i bigint , b boolean )\nINPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\nOUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"
  }
}
