package io.eels.component.hive

import java.io.File
import java.util.UUID

import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema.{BooleanType, Field, IntType, StringType, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.util.Random

class HiveCompactTest extends FunSuite with HiveConfig with Matchers with BeforeAndAfterAll {

  val dbname = "sam"
  val table = "compact_test_" + System.currentTimeMillis()

  override def afterAll(): Unit = {
    HiveTable(dbname, table).drop()
  }

  test("compact should result in a single file") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)

    HiveTable(dbname, table).drop()

    val schema = StructType(
      Field("a", StringType),
      Field("b", StringType),
      Field("c", IntType.Signed),
      Field("d", BooleanType)
    )
    def createRow = Row(schema, Seq(UUID.randomUUID.toString, UUID.randomUUID.toString, Random.nextInt(1000000), Random.nextBoolean))

    val sink = HiveSink(dbname, table).withCreateTable(true)
    val size = 10000

    DataStream.fromIterator(schema, Iterator.continually(createRow).take(size)).to(sink, 4)

    val t = HiveTable(dbname, table)
    t.paths(false, false).size shouldBe 4
    // t.compact()
    //    t.paths(false, false).size shouldBe 1
    //   t.stats().rows shouldBe size
  }
}
