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
    Row(schema3, Vector("Elton John", "m")),
    Row(schema3, Vector("Kate Bush", "f"))
  )

  "DataStream.filter" should {
    "return only matching rows" in {
      source.toDataStream().filter(row => row.values.contains("Kent")).collect.size shouldBe 22
    }
    "support row filtering by column name and fn" in {
      ds3.filter("gender", _ == "f").size shouldBe 1
    }
    "throw if the column does not exist" in {
      intercept[RuntimeException] {
        ds1.filter("qweeg", _ == "1").size
      }
    }
  }

  "DataStream.projection" should {
    "support column projection" in {
      val ds = DataStream.fromValues(
        StructType("name", "location"),
        Vector(
          List("sam", "aylesbury"),
          List("jam", "aylesbury"),
          List("ham", "buckingham")
        )
      )
      val f = ds.projection("location")
      f.head.values shouldBe Seq("aylesbury")
      f.schema shouldBe StructType("location")
    }
    "support column projection expressions" in {
      val ds = DataStream.fromValues(
        StructType("name", "location"),
        Vector(
          List("sam", "aylesbury"),
          List("jam", "aylesbury"),
          List("ham", "buckingham")
        )
      )
      val f = ds.projectionExpression("location,name")
      f.head.values shouldBe Vector("aylesbury", "sam")
      f.schema shouldBe StructType("location", "name")
    }
    "support column projection re-ordering" in {
      val ds = DataStream.fromValues(
        StructType("name", "location"),
        Vector(
          List("sam", "aylesbury"),
          List("jam", "aylesbury"),
          List("ham", "buckingham")
        )
      )
      val f = ds.projection("location", "name")
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

  "DataStream.replace" should {
    "work!" in {
      val ds1 = DataStream.fromValues(
        StructType("name", "location"),
        Seq(
          List("sam", "aylesbury"),
          List("ham", "buckingham")
        )
      )
      ds1.replace("sam", "jam").toSet shouldBe {
        Set(
          Row(ds1.schema, "jam", "aylesbury"),
          Row(ds1.schema, "ham", "buckingham")
        )
      }
    }
    "support replace by field" in {
      val ds1 = DataStream.fromValues(
        StructType("a", "b"),
        Seq(
          List("sam", "sam"),
          List("ham", "jam")
        )
      )
      ds1.replace("a", "sam", "ram").toSet shouldBe {
        Set(
          Row(ds1.schema, "ram", "sam"),
          Row(ds1.schema, "ham", "jam")
        )
      }
    }
    "support replace by field with function" in {
      val ds = DataStream.fromValues(
        StructType("a", "b"),
        Seq(
          List("sam", "sam"),
          List("ham", "jam")
        )
      )
      val fn: Any => Any = any => any.toString.reverse
      ds.replace("a", fn).toSet shouldBe
        Set(
          Row(ds.schema, "mas", "sam"),
          Row(ds.schema, "mah", "jam")
        )
    }
    "throw exception if field does not exist" in {
      intercept[IllegalArgumentException] {
        ds3.replace("wibble", "a", "b").collect
      }
      intercept[IllegalArgumentException] {
        val fn: Any => Any = x => x
        ds3.replace("wibble", fn).collect
      }
    }
  }

  "DataStream.exists" should {
    "return true when a row matches the predicate" in {
      ds3.exists(row => row.containsValue("Elton John")) shouldBe true
      ds3.exists(row => row.containsValue("Jack Bruce")) shouldBe false
    }
  }

  "DataStream.find" should {
    "return Option[Row] when a row matches the predicate" in {
      ds3.find(row => row.containsValue("Elton John")) shouldBe Option(Row(ds3.schema, Seq("Elton John", "m")))
    }
    "return None when no row matches the predicate" in {
      ds3.find(row => row.containsValue("Jack Bruce")) shouldBe None
    }
  }

  "DataStream.dropNullRows" should {
    "work" in {
      val ds1 = DataStream.fromValues(
        StructType("name", "location"),
        Seq(
          List("sam", null),
          List("ham", "buckingham")
        )
      )
      ds1.dropNullRows.collect shouldBe List(Row(ds1.schema, "ham", "buckingham"))
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
    "accept regex" in {
      val schema = StructType(Field("aaa", StringType), Field("aab", LongType(true)))
      val ds1 = DataStream.fromValues(schema, Vector(Vector("a", 1), Vector("b", 2)))

      val ds2 = ds1.replaceFieldType("aa.".r, BooleanType)
      ds2.schema shouldBe StructType(Field("aaa", BooleanType), Field("aab", BooleanType))

      val ds3 = ds1.replaceFieldType(".*b".r, BooleanType)
      ds3.schema shouldBe StructType(Field("aaa", StringType), Field("aab", BooleanType))
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

  "DataStream.stripCharsFromFieldNames" should {
    "remove offending characters" in {
      val ds = DataStream.fromValues(
        StructType("name", "#location", "!postcode"),
        Vector(
          List("sam", "aylesbury", "hp22"),
          List("ham", "buckingham", "mk10")
        )
      )
      ds.stripCharsFromFieldNames(Seq('#', '!', 'p')).schema shouldBe
        StructType("name", "location", "ostcode")
    }
  }

  "DataStream.addField with function" should {
    "invoke function for each row to return new row" in {
      val ds = DataStream.fromValues(
        StructType("a", "b"),
        Vector(
          List("1", "2"),
          List("3", "4")
        )
      )
      ds.addField(Field("c"), (row: Row) => row("b").toString.toInt + 1).collect shouldBe Seq(
        Row(ds.schema.addField("c"), List("1", "2", 3)),
        Row(ds.schema.addField("c"), List("3", "4", 5))
      )
    }
  }

  "DataStream.multiplex" should {
    "return multiple independant branches of the stream" in {
      val ds = DataStream.fromIterator(
        StructType("a"),
        Iterator.tabulate(5)(k => Row(StructType("a"), Seq(k.toString)))
      )
      val multis = ds.multiplex(2)
      multis.head.take(2).collect.map(_.values) shouldBe Vector(List("0"), List("1"))
      multis.last.drop(2).collect.map(_.values) shouldBe Vector(List("2"), List("3"), List("4"))
    }
  }

  "DataStream.removeField" should {
    "remove column" in {
      val ds1 = DataStream.fromValues(
        StructType("name", "location", "postcode"),
        Seq(
          List("sam", "aylesbury", "hp22"),
          List("ham", "buckingham", "mk10")
        )
      )
      val ds2 = ds1.removeField("location")
      ds2.schema shouldBe StructType("name", "postcode")
      ds2.toSet shouldBe Set(Row(ds2.schema, "sam", "hp22"), Row(ds2.schema, "ham", "mk10"))
    }
    "not remove column if case is different" in {
      val ds1 = DataStream.fromValues(
        StructType("name", "location", "postcode"),
        Seq(
          List("sam", "aylesbury", "hp22"),
          List("ham", "buckingham", "mk10")
        )
      )
      val ds2 = ds1.removeField("POSTcode")
      ds2.schema shouldBe StructType("name", "location", "postcode")
      ds2.toSet shouldBe Set(Row(ds1.schema, "sam", "aylesbury", "hp22"), Row(ds1.schema, "ham", "buckingham", "mk10"))
    }
    "remove column with ignore case" in {
      val ds1 = DataStream.fromValues(
        StructType("name", "location", "postcode"),
        Seq(
          List("sam", "aylesbury", "hp22"),
          List("ham", "buckingham", "mk10")
        )
      )
      val ds2 = ds1.removeField("locATION", false)
      ds2.schema shouldBe StructType("name", "postcode")
      ds2.toSet shouldBe Set(Row(ds2.schema, "sam", "hp22"), Row(ds2.schema, "ham", "mk10"))
    }
  }

  "DataStream.iterator" should {
    "allow iteration over the values" in {
      ds1.iterator.toSeq.size shouldBe 10
    }
  }

  "DataStream.cartesian" should {
    "create the cartesian join of two ds" in {
      val schema = ds2.schema.concat(ds3.schema)
      ds2.cartesian(ds3).collect shouldBe
      Seq(
        Row(schema, Seq("Pizza", "Italy", "Elton John", "m")),
        Row(schema, Seq("Pizza", "Italy", "Kate Bush", "f")),
        Row(schema, Seq("Foie Gras", "France", "Elton John", "m")),
        Row(schema, Seq("Foie Gras", "France", "Kate Bush", "f"))
      )
    }
  }

  "DataStream.subtract" should {
    "keep only the rows in the lhs that don't exist in the rhs" in {
      ds2.substract(ds2.take(1)).collect shouldBe
        Seq(
          Row(ds2.schema, Seq("Foie Gras", "France"))
        )
    }
  }

  "DataStream.intersection" should {
    "keep only the rows in the lhs that exist in the rhs" in {
      ds2.intersection(ds2.take(1)).collect shouldBe
        Seq(
          Row(ds2.schema, Seq("Pizza", "Italy"))
        )
    }
  }

  "DataStream.apply" should {
    "convert from a Seq[T<:Product]" in {

      val s1 = Student("name1", 2, 1.2, true)
      val s2 = Student("name2", 3, 11.2, true)
      val students = Seq(s1, s2)

      val ds = DataStream(students)

      val rows = ds.collect
      rows.size shouldBe 2
      rows shouldBe Seq(
        Row(ds.schema, s1.productIterator.toSeq),
        Row(ds.schema, s2.productIterator.toSeq)
      )
    }
  }
}

case class Student(name: String, age: Int, height: Double, hungary: Boolean)
