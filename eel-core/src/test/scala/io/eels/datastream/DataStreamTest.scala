package io.eels.datastream

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger

import io.eels.Row
import io.eels.component.csv.CsvSource
import io.eels.schema.{BooleanType, Field, IntType, LongType, StringType, StructType}
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

  val schema3 = StructType(
    Field("artist"),
    Field("gender")
  )
  val ds3 = DataStream.fromRows(schema3,
    Row(schema2, Vector("Elton John", "m")),
    Row(schema2, Vector("Kate Bush", "f"))
  )

  "DataStream.filter" should {
    "return only matching rows" in {
      source.toDataStream().filter(row => row.values.contains("Kent")).collect.size shouldBe 22
    }
    "throw if the column does not exist" in {
      intercept[RuntimeException] {
        ds1.filter("qweeg", _ == "1").size
      }
    }
  }

  "DataStream.projection" should {
    "support column projection" in {
      val frame = DataStream.fromValues(
        StructType("name", "location"),
        Vector(
          List("sam", "aylesbury"),
          List("jam", "aylesbury"),
          List("ham", "buckingham")
        )
      )
      val f = frame.projection("location")
      f.head.values shouldBe Seq("aylesbury")
      f.schema shouldBe StructType("location")
    }
    "support column projection expressions" in {
      val frame = DataStream.fromValues(
        StructType("name", "location"),
        Vector(
          List("sam", "aylesbury"),
          List("jam", "aylesbury"),
          List("ham", "buckingham")
        )
      )
      val f = frame.projectionExpression("location,name")
      f.head.values shouldBe Vector("aylesbury", "sam")
      f.schema shouldBe StructType("location", "name")
    }
    "support column projection re-ordering" in {
      val frame = DataStream.fromValues(
        StructType("name", "location"),
        Vector(
          List("sam", "aylesbury"),
          List("jam", "aylesbury"),
          List("ham", "buckingham")
        )
      )
      val f = frame.projection("location", "name")
      f.schema shouldBe StructType("location", "name")
      f.head.values shouldBe Vector("aylesbury", "sam")
    }
  }

  "DataStream.concat" should {
    "merge together two datastreams" in {
      ds1.take(2).concat(ds2).collect.map(_.values) shouldBe
        Vector(Vector("Elton John", 1969, "Empty Sky", 1433, "Pizza", "Italy"), Vector("Elton John", 1971, "Madman Across the Water", 7636, "Foie Gras", "France"))
    }
  }

  "DataStream.join" should {
    "merge schemas together" in {
      ds1.join("artist", ds3).schema shouldBe StructType(
        Field("artist"),
        Field("year", IntType()),
        Field("album"),
        Field("sales", LongType()),
        Field("gender")
      )
    }
    "join together two datastreams on a key" in {
      ds1.take(2).join("artist", ds3).collect.map(_.values) shouldBe
        Vector(Vector("Elton John", 1969, "Empty Sky", 1433, "m"), Vector("Elton John", 1971, "Madman Across the Water", 7636, "m"))
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

  "DataStream.dropWhile" should {
    "work!" in {
      ds1.dropWhile(_.get("artist") == "Elton John").collect.map(_.values) shouldBe
        Vector(
          Vector("Kate Bush", 1978, "The Kick Inside", 2577),
          Vector("Kate Bush", 1978, "Lionheart", 745),
          Vector("Kate Bush", 1980, "Never for Ever", 7444),
          Vector("Kate Bush", 1982, "The Dreaming", 8253),
          Vector("Kate Bush", 1985, "Hounds of Love", 2495)
        )
    }
    "support with column predicate" in {
      ds1.dropWhile("year", value => value.toString.toInt < 1980).collect.map(_.values) shouldBe
        Vector(
          Vector("Kate Bush", 1980, "Never for Ever", 7444),
          Vector("Kate Bush", 1982, "The Dreaming", 8253),
          Vector("Kate Bush", 1985, "Hounds of Love", 2495)
        )
    }
  }

  "DataStream.explode" should {
    "expand rows" in {
      ds3.explode(row => Seq(row, row)).collect.map(_.values) shouldBe
        Vector(
          Vector("Elton John", "m"),
          Vector("Elton John", "m"),
          Vector("Kate Bush", "f"),
          Vector("Kate Bush", "f")
        )
    }
  }

  "DataStream.replaceNullValues" should {
    "replace null values" in {
      val ds = DataStream.fromValues(
        StructType("name", "location"),
        Vector(
          List("sam", null),
          List("jam", "aylesbury"),
          List(null, "buckingham")
        )
      )
      ds.replaceNullValues("wibble").collect.map(_.values) shouldBe
        Vector(
          List("sam", "wibble"),
          List("jam", "aylesbury"),
          List("wibble", "buckingham")
        )
    }
  }

  "DataStream.join" should {
    "join two datastreams on a shared key" in {
      source.toDataStream().drop(32).collect.size shouldBe 468
    }
  }

  "DataStream.union" should {
    "join together two datastreams" in {
      ds1.take(2).union(ds1.take(1)).collect.map(_.values) shouldBe
        Vector(Vector("Elton John", 1969, "Empty Sky", 1433), Vector("Elton John", 1971, "Madman Across the Water", 7636), Vector("Elton John", 1969, "Empty Sky", 1433))
    }
  }

  "DataStream.take" should {
    "return only n number of rows" in {
      source.toDataStream().take(3).collect.map(_.values) shouldBe
        Vector(
          Vector("Aleshia", "Tomkiewicz", "Alan D Rosenburg Cpa Pc", "14 Taylor St", "St. Stephens Ward", "Kent", "CT2 7PP", "01835-703597", "01944-369967", "atomkiewicz@hotmail.com", "http://www.alandrosenburgcpapc.co.uk"),
          Vector("Evan", "Zigomalas", "Cap Gemini America", "5 Binney St", "Abbey Ward", "Buckinghamshire", "HP11 2AX", "01937-864715", "01714-737668", "evan.zigomalas@gmail.com", "http://www.capgeminiamerica.co.uk"),
          Vector("France", "Andrade", "Elliott, John W Esq", "8 Moor Place", "East Southbourne and Tuckton W", "Bournemouth", "BH6 3BE", "01347-368222", "01935-821636", "france.andrade@hotmail.com", "http://www.elliottjohnwesq.co.uk")
        )
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

  "DataStream.addFieldIfNotExists" should {
    "not add column if already exists" in {
      val ds = ds1.addFieldIfNotExists("artist", "bibble")
      ds.schema shouldBe schema1
      ds.head.values shouldBe Vector("Elton John", 1969, "Empty Sky", 1433)
    }
    "add column if it does not exist" in {
      val ds = ds3.addFieldIfNotExists("testy", "bibble")
      ds.schema shouldBe StructType(
        Field("artist"),
        Field("gender"),
        Field("testy")
      )
      ds.head.values shouldBe Vector("Elton John", "m", "bibble")
    }
  }

  "DataStream.replaceFieldType" should {
    "replace matching types in schema" in {
      val schema = StructType(Field("a", StringType), Field("b", LongType(true)))
      val ds1 = DataStream.fromValues(schema, Vector(Vector("a", 1), Vector("b", 2)))
      val ds2 = ds1.replaceFieldType(StringType, BooleanType)
      ds2.schema shouldBe StructType(Field("a", BooleanType), Field("b", LongType(true)))
      ds2.collect.map(_.values) shouldBe Seq(Vector("a", 1), Vector("b", 2))
    }
  }

  "DataStream.addField" should {
    "support adding columns" in {
      val ds2 = ds3.addField("testy", "bibble")
      ds2.schema shouldBe StructType("artist", "gender", "testy")
      ds2.head.values shouldBe Vector("Elton John", "m", "bibble")
    }
  }

  "DataStream.foreach" should {
    "execute for every row" in {
      val count = new AtomicInteger(0)
      ds1.foreach(_ => count.incrementAndGet).size
      count.get() shouldBe 10
    }
  }

  "Frame.stripFromFieldNames" should {
    "remove offending characters" in {
      val frame = DataStream.fromValues(
        StructType("name", "#location", "!postcode"),
        Vector(
          List("sam", "aylesbury", "hp22"),
          List("ham", "buckingham", "mk10")
        )
      )
      frame.stripCharsFromFieldNames(Seq('#', '!', 'p')).schema shouldBe
        StructType("name", "location", "ostcode")
    }
  }
}
