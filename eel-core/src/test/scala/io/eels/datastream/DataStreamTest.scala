package io.eels.datastream

import java.nio.file.Paths

import io.eels.Row
import io.eels.component.csv.CsvSource
import io.eels.schema.{Field, IntType, LongType, StructType}
import org.scalatest.{Matchers, WordSpec}

class DataStreamTest extends WordSpec with Matchers {

  val file = getClass.getResource("/uk-500.csv").toURI()
  val source = CsvSource(Paths.get(file))

  val schema1 = StructType(
    Field("artist"),
    Field("year", IntType()),
    Field("album"),
    Field("sales", LongType())
  )
  val ds1 = DataStream.fromRows(schema1,
    Row(schema1, Vector("Elton John", 1969, "Empty Sky", 1433)),
    Row(schema1, Vector("Elton John", 1971, "Madman Across the Water", 7636)),
    Row(schema1, Vector("Elton John", 1972, "Honky Château", 2525)),
    Row(schema1, Vector("Elton John", 1973, "Goodbye Yellow Brick Road", 4352)),
    Row(schema1, Vector("Elton John", 1975, "Rock of the Westies", 5645)),
    Row(schema1, Vector("Kate Bush", 1978, "The Kick Inside", 2577)),
    Row(schema1, Vector("Kate Bush", 1978, "Lionheart", 745)),
    Row(schema1, Vector("Kate Bush", 1980, "Never for Ever", 7444)),
    Row(schema1, Vector("Kate Bush", 1982, "The Dreaming", 8253)),
    Row(schema1, Vector("Kate Bush", 1985, "Hounds of Love", 2495))
  )

  val schema2 = StructType(
    Field("name"),
    Field("country")
  )
  val ds2 = DataStream.fromRows(schema2,
    Row(schema2, Vector("Pizza", "Italy")),
    Row(schema2, Vector("Foie Gras", "France"))
  )

  "DataStream.filter" should {
    "return only matching rows" in {
      source.toDataStream().filter(row => row.values.contains("Kent")).collect.size shouldBe 22
    }
  }

  "DataStream.concat" should {
    "join together two datastreams" in {
      ds1.take(2).concat(ds2).collect.map(_.values) shouldBe
        Vector(Vector("Elton John", 1969, "Empty Sky", 1433, "Pizza", "Italy"), Vector("Elton John", 1971, "Madman Across the Water", 7636, "Foie Gras", "France"))
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
      source.toDataStream().take(4).collect.size shouldBe 4
    }
  }

  "DataStream.takeWhile" should {
    "support take while with row predicate" in {
      source.toDataStream().takeWhile(row => row.values.mkString.contains("co.uk")).collect.size shouldBe 6
    }
    "support take while with column predicate" in {
      source.toDataStream().takeWhile("web", _.toString.contains("co.uk")).collect.size shouldBe 6
    }
  }

  "DataStream.renameField" should {
    "update the schema" in {
      source.toDataStream().renameField("web", "website").schema.fieldNames() shouldBe
        Vector("first_name", "last_name", "company_name", "address", "city", "county", "postal", "phone1", "phone2", "email", "website")
    }
    "copy the rows with updated schema" in {
      source.toDataStream().renameField("web", "website").take(1).collect.head.schema.fieldNames() shouldBe
        Vector("first_name", "last_name", "company_name", "address", "city", "county", "postal", "phone1", "phone2", "email", "website")
    }
  }

  "DataStream.withLowerCaseSchema" should {
    "return same values but with lower case schema" in {
      val schema = StructType("A", "B", "c")
      val f = DataStream.fromValues(schema, Vector(Vector("x", "Y", null))).withLowerCaseSchema()
      f.schema shouldBe StructType("a", "b", "c")
      f.collect shouldBe Seq(Row(f.schema, Vector("x", "Y", null)))
    }
  }
}
