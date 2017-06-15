package io.eels.dataframe

import java.nio.file.Paths

import io.eels.component.csv.CsvSource
import org.scalatest.{Matchers, WordSpec}

class DataStreamTest extends WordSpec with Matchers {

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

  "DataStream.drop" should {
    "drop required number of rows" in {
      source.toDataStream().drop(32).collect.size shouldBe 468
    }
  }

  "DataStream.take" should {
    "return only n number of rows" in {
      source.toDataStream.take(4).collect.size shouldBe 4
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

  "DataStream.renameField" should {
    "update the schema" in {
      source.toDataStream.renameField("web", "website").schema.fieldNames() shouldBe
        Vector("first_name", "last_name", "company_name", "address", "city", "county", "postal", "phone1", "phone2", "email", "website")
    }
    "copy the rows with updated schema" in {
      source.toDataStream.renameField("web", "website").take(1).collect.head.schema.fieldNames() shouldBe
        Vector("first_name", "last_name", "company_name", "address", "city", "county", "postal", "phone1", "phone2", "email", "website")
    }
  }
}
