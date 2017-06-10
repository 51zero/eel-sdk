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

  "DataFrame.drop" should {
    "drop required number of rows" in {
      source.toDataStream().drop(66).collect.size shouldBe 434
    }
  }
}
