package io.eels.component.hive

import java.io.File

import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema.{Field, StringType, StructType}
import org.scalatest.{FunSuite, Matchers}

import scala.util.{Random, Try}

class HiveTableTest extends FunSuite with Matchers {

  import HiveConfig._

  val dbname = "default"
  val table = "test_table_" + System.currentTimeMillis()

  Try {
    HiveTable(dbname, table).drop()
  }

  test("partition values should return values for the matching key") {
    assume(new File(s"$basePath/core-site.xml").exists)

    val schema = StructType(
      Field("a", StringType),
      Field("b", StringType),
      Field("c", StringType)
    )
    def createRow = Row(schema,
      Seq(
        Random.shuffle(List("a", "b", "c")).head,
        Random.shuffle(List("x", "y", "z")).head,
        Random.shuffle(List("q", "r", "s")).head
      )
    )

    val sink = HiveSink(dbname, table).withCreateTable(true, Seq("a", "b"))
    val size = 1000

    DataStream.fromIterator(schema, Iterator.continually(createRow).take(size)).to(sink, 4)

    HiveTable(dbname, table).partitionValues("b") shouldBe Set("x", "y", "z")
  }
}
