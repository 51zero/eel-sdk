package io.eels.dataframe

import java.nio.file.Paths

import io.eels.component.csv.CsvSource
import org.scalatest.{Matchers, WordSpec}

class DataStreamTest extends WordSpec with Matchers {

  implicit val em = ExecutionManager.local
  val file = getClass.getResource("/uk-500.csv").toURI()
  val source = CsvSource(Paths.get(file))

  "DataStream.filter" should {
    "return only matching rows" in {
      source.toDataStream().filter(row => row.values.contains("Kent")).collect.size shouldBe 22
    }
  }

  "DataStream.map" should {
    "transform each row" in {
      source.toDataStream().map(row => row.add("foo", "moo")).take(1).collect.flatMap(_.values) should contain("moo")
    }
  }

  "DataStream.take" should {
    "keep only the required number of rows" in {
      source.toDataStream().take(3).collect.size shouldBe 3
    }
  }

  "DataStream.drop" should {
    "drop required number of rows" in {
      source.toDataStream().drop(32).collect.size shouldBe 468
    }
  }

  "DataStream.takeWhile" should {
    "support take while with row predicate" in {
      source.toDataStream.takeWhile(row => row.values.mkString.contains("co.uk")).collect.size shouldBe 6
    }
    "support take while with column predicate" in {
      source.toDataStream.takeWhile("web", _.toString.contains("co.uk")).collect.size shouldBe 6
    }
  }

  "DataStream.takeUntil" should {
    "support take until with row predicate" in {
      source.toDataStream.takeUntil(row => row.values.mkString.contains(".de")).collect.size shouldBe 7
    }
    "support take until with column predicate" in {
      source.toDataStream.takeUntil("web", _.toString.contains(".de")).collect.size shouldBe 7
    }
  }
}
