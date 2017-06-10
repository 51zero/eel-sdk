package io.eels.dataframe

import java.nio.file.Paths

import io.eels.component.csv.CsvSource
import org.scalatest.{Matchers, WordSpec}

class DataFrameTest extends WordSpec with Matchers {

  implicit val em = ExecutionManager.local
  val file = getClass.getResource("/uk-500.csv").toURI()
  val source = CsvSource(Paths.get(file))

  "DataFrame.filter" should {
    "return only matching rows" in {
      source.toDataStream().filter(row => row.values.contains("Kent")).collect.size shouldBe 22
    }
  }

  "DataFrame.map" should {
    "transform each row" in {
      source.toDataStream().map(row => row.add("foo", "moo")).take(1).collect.flatMap(_.values) should contain("moo")
    }
  }

  "DataFrame.take" should {
    "keep only the required number of rows" in {
      source.toDataStream().take(3).collect.size shouldBe 3
    }
  }

  "DataFrame.drop" should {
    "drop required number of rows" in {
      source.toDataStream().drop(32).collect.size shouldBe 468
    }
  }
}
