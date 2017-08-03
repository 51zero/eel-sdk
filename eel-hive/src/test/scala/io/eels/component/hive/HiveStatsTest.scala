package io.eels.component.hive

import java.io.File

import io.eels.datastream.DataStream
import io.eels.schema._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.util.{Random, Try}

class HiveStatsTest extends FunSuite with Matchers with BeforeAndAfterAll {

  import HiveConfig._

  private val dbname = "sam"
  private val table = "stats_test_" + System.currentTimeMillis()
  private val partitioned_table = "stats_test2_" + System.currentTimeMillis()

  val schema = StructType(
    Field("a", StringType),
    Field("b", IntType.Signed)
  )
  def createRow = Seq(Random.shuffle(List("a", "b", "c")).head, Random.shuffle(List(1, 2, 3, 4, 5)).head)

  val amount = 10000

  override def afterAll(): Unit = Try {
    HiveTable(dbname, table).drop()
    HiveTable(dbname, partitioned_table).drop()
  }

  Try {
    DataStream.fromIterator(schema, Iterator.continually(createRow).take(amount))
      .to(HiveSink(dbname, table).withCreateTable(true), 4)

    DataStream.fromIterator(schema, Iterator.continually(createRow).take(amount))
      .to(HiveSink(dbname, partitioned_table).withCreateTable(true, Seq("a")), 4)
  }

  test("stats should return row counts for a non-partitioned table") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)
    HiveTable(dbname, table).stats().count shouldBe amount
  }

  test("stats should return row counts for a partitioned table") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)
    HiveTable(dbname, partitioned_table).stats().count shouldBe amount
  }

  test("stats should throw exception when constraints specified on a non-partitioned table") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)
    intercept[RuntimeException] {
      val constraints = Seq(PartitionConstraint.equals("a", "b"))
      HiveTable(dbname, table).stats().count(constraints)
    }
  }

  test("stats should support row count constraints for a partitioned table") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)
    val constraints = Seq(PartitionConstraint.equals("a", "b"))
    HiveTable(dbname, partitioned_table).stats().count(constraints) > 0 shouldBe true
    HiveTable(dbname, partitioned_table).stats().count(constraints) should be < amount.toLong
  }

  test("stats should support min and max for a non-partitioned tabled") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)
    HiveTable(dbname, table).stats.max("b") shouldBe 5
    HiveTable(dbname, table).stats.min("b") shouldBe 1
  }

  test("stats should support min and max for a partitioned table") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)
    HiveTable(dbname, partitioned_table).stats.max("b") shouldBe 5
    HiveTable(dbname, partitioned_table).stats.min("b") shouldBe 1
  }
}
