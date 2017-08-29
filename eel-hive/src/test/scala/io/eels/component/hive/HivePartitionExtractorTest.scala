package io.eels.component.hive

import io.eels.Row
import io.eels.schema.{Partition, PartitionEntry, StructType}
import org.scalatest.{FunSuite, Matchers}

class HivePartitionExtractorTest extends FunSuite with Matchers {

  test("HivePartitionExtractor should extract single partitions") {
    val schema = StructType("artist", "album", "year")
    val extractor = new HivePartitionExtractor(schema, Seq("artist"))
    val row = Row(schema, Vector("elton", "yellow brick road ", 1972))
    extractor(row) shouldBe Partition(PartitionEntry("artist", "elton"))
  }

  test("HivePartitionExtractor should extract multiple partitions in partition key order") {
    val schema = StructType("artist", "album", "year")
    val extractor = new HivePartitionExtractor(schema, Seq("year", "artist"))
    val row = Row(schema, Vector("elton", "yellow brick road ", 1972))
    extractor(row) shouldBe Partition(PartitionEntry("year", "1972"), PartitionEntry("artist", "elton"))
  }
}
