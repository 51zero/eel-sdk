package io.eels.component.hive

import io.eels.{Column, Frame, Schema, SchemaType}
import io.eels.testkit.HiveTestKit
import org.scalatest.{Matchers, WordSpec}

class HiveSourceTest extends WordSpec with Matchers with HiveTestKit {

  import scala.concurrent.ExecutionContext.Implicits.global

  val schema = Schema(Column("a"), Column("b"), Column("c"), Column("p"), Column("q"))
  HiveOps.createTable("sam", "hivesourcetest", schema, format = HiveFormat.Parquet, partitionKeys = List("p", "q"))

  Frame(schema, Array("1", "2", "3", "4", "5"), Array("4", "5", "6", "7", "8")).to(HiveSink("sam", "hivesourcetest"))

  "HiveSource.partitionValues" should {
    "return all partition values directly from metastore" in {
      HiveSource("sam", "hivesourcetest").partitionValues("p").toSet shouldBe Set("7", "4")
    }
  }

  "HiveSource.schema" should {
    "include partitions as non null columns" in {
      HiveSource("sam", "hivesourcetest").schema shouldBe
        Schema(List(Column("a", SchemaType.String, true), Column("b", SchemaType.String, true), Column("c", SchemaType.String, true), Column("p", SchemaType.String, false), Column("q", SchemaType.String, false)))
    }
    "respect withColumns" in {
      HiveSource("sam", "hivesourcetest").withColumns("c", "b").schema shouldBe
        Schema(List(Column("c", SchemaType.String, true), Column("b", SchemaType.String, true)))
    }
    "respect withColumns that specify partitions" in {
      HiveSource("sam", "hivesourcetest").withColumns("c", "q").schema shouldBe
        Schema(List(Column("c", SchemaType.String, true), Column("q", SchemaType.String, false)))
    }
    "respect withColumns that specify ONLY partitions" in {
      HiveSource("sam", "hivesourcetest").withColumns("q", "p").schema shouldBe Schema(List(Column("q", SchemaType.String, false), Column("p", SchemaType.String, false)))
    }
    "respect withColumns that specify ONLY some partitions" in {
      HiveSource("sam", "hivesourcetest").withColumns("q").schema shouldBe Schema(List(Column("q", SchemaType.String, false)))
    }
  }

  "HiveSource.withColumns" should {
    "return rows in projection order" in {
      HiveSource("sam", "hivesourcetest").withColumns("c", "b").toSet.map(_.values) shouldBe
        Set(Vector("3", "2"), Vector("6", "5"))
    }
    "return rows in projection order for a projection that includes partitions" in {
      HiveSource("sam", "hivesourcetest").withColumns("c", "q", "a").toSet.map(_.values) shouldBe
        Set(Vector("3", "5", "1"), Vector("6", "8", "4"))
    }
    "return rows in projection order for a projection that includes ONLY partitions" in {
      HiveSource("sam", "hivesourcetest").withColumns("q", "p").toSet.map(_.values) shouldBe
        Set(Vector("5", "4"), Vector("8", "7"))
    }
    "return rows in projection order for a projection that includes ONLY some partitions" in {
      HiveSource("sam", "hivesourcetest").withColumns("q").toSet.map(_.values) shouldBe Set(Vector("5"), Vector("8"))
      HiveSource("sam", "hivesourcetest").withColumns("p").toSet.map(_.values) shouldBe Set(Vector("4"), Vector("7"))
    }
  }

  "HiveSource" should {
    "return rows with same ordering as schema" in {
      HiveSource("sam", "hivesourcetest").toSet.map(_.values) shouldBe
        Set(Vector("1", "2", "3", "4", "5"), Vector("4", "5", "6", "7", "8"))
    }
  }
}
