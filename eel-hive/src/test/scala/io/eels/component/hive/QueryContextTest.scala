package io.eels.component.hive

import java.io.File
import java.nio.file.Paths

import com.sksamuel.exts.io.RecursiveDelete
import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema.{Field, IntType, StringType, StructType}
import org.scalatest.{FunSuite, Matchers}

import scala.util.{Random, Try}

class QueryContextTest extends FunSuite with Matchers with HiveConfig {

  val dbname = "sam"
  val table = "query_test_" + System.currentTimeMillis()

  Try {
    RecursiveDelete(Paths.get("metastore_db"))
  }

  test("return min and max") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)

    val schema = StructType(
      Field("a", StringType),
      Field("b", IntType.Signed)
    )
    def createRow = Row(schema, Seq(Random.shuffle(List("a", "b", "c")).head, Random.shuffle(List(1, 2, 3, 4, 5)).head))

    val sink = HiveSink(dbname, table).withCreateTable(true)
    val size = 10000

    DataStream.fromIterator(schema, Iterator.continually(createRow).take(size)).to(sink, 10)

    HiveTable(dbname, table).queryContext(Nil).max("b") shouldBe 5
    HiveTable(dbname, table).queryContext(Nil).min("b") shouldBe 1
  }

  ignore("return min and max for strings") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)

    HiveTable(dbname, table).drop()

    val schema = StructType(
      Field("a", StringType),
      Field("b", StringType)
    )
    def createRow = Row(schema, Seq(Random.shuffle(List("a", "b", "c")).head, Random.shuffle(List("a", "z")).head))

    val sink = HiveSink(dbname, table).withCreateTable(true)
    val size = 10000

    DataStream.fromIterator(schema, Iterator.continually(createRow).take(size)).to(sink, 10)

    HiveTable(dbname, table).queryContext(Nil).max("b") shouldBe 5
    HiveTable(dbname, table).queryContext(Nil).min("b") shouldBe 1
  }
}
