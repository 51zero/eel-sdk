package io.eels.component.hive

import java.io.File

import io.eels.component.hive.partition.DynamicPartitionStrategy
import io.eels.datastream.DataStream
import io.eels.schema.{Field, Partition, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.util.Try

class HiveDynamicPartitionTest extends FunSuite with HiveConfig with Matchers with BeforeAndAfterAll {

  val dbname = "sam"
  val table = "dynp_test_" + System.currentTimeMillis()

  val schema = StructType(Field("a"), Field("b"))

  Try {
    HiveTable(dbname, table).create(schema, Seq("a"))
  }

  override def afterAll(): Unit = Try {
    HiveTable(dbname, table).drop()
  }

  test("dynamic partition strategy should create new partitions") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)
    HiveTable(dbname, table).partitionValues("a") shouldBe Set.empty
    DataStream.fromValues(schema, Seq(Seq("1", "2"), Seq("3", "4"))).to(HiveSink(dbname, table))
    HiveTable(dbname, table).partitionValues("a") shouldBe Set("1", "3")
  }

  test("skip partition if partition already exists") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)
    new DynamicPartitionStrategy().ensurePartition(Partition("a" -> "1"), dbname, table, false, client)
    new DynamicPartitionStrategy().ensurePartition(Partition("a" -> "1"), dbname, table, false, client)
  }
}
