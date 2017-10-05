package io.eels.component.csv

import java.nio.file.Paths

import io.eels.schema.{Field, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.{Matchers, WordSpec}

class CsvSourceTest extends WordSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(conf)

  "CsvSource" should {
    "read schema" in {
      val file = getClass.getResource("/io/eels/component/csv/csvtest.csv").toURI()
      val path = Paths.get(file)
      CsvSource(path).schema shouldBe StructType(
        Field("a", StringType, true),
        Field("b", StringType, true),
        Field("c", StringType, true)
      )
    }
    "support null cell value option as null" in {
      val file = getClass.getResource("/io/eels/component/csv/csvwithempty.csv").toURI()
      val path = Paths.get(file)
      CsvSource(path).withNullValue(null).toDataStream().toSet.map(_.values) shouldBe
        Set(Vector("1", null, "3"))
    }
    "support null cell value replacement value" in {
      val file = getClass.getResource("/io/eels/component/csv/csvwithempty.csv").toURI()
      val path = Paths.get(file)
      CsvSource(path).withNullValue("foo").toDataStream().toSet.map(_.values) shouldBe
        Set(Vector("1", "foo", "3"))
    }
    "read from path" in {
      val file = getClass.getResource("/io/eels/component/csv/csvtest.csv").toURI()
      val path = Paths.get(file)
      CsvSource(path).withHeader(Header.FirstRow).toDataStream().size shouldBe 3
      CsvSource(path).withHeader(Header.None).toDataStream().size shouldBe 4
    }
    "allow specifying manual schema" in {
      val file = getClass.getResource("/io/eels/component/csv/csvtest.csv").toURI()
      val path = Paths.get(file)
      val schema = StructType(
        Field("test1", StringType, true),
        Field("test2", StringType, true),
        Field("test3", StringType, true)
      )
      CsvSource(path).withSchema(schema).toDataStream().schema shouldBe schema
    }
    "support reading header" in {
      val file = getClass.getResource("/io/eels/component/csv/csvtest.csv").toURI()
      val path = Paths.get(file)
      CsvSource(path).withHeader(Header.FirstRow).toDataStream().collect.map(_.values).toSet shouldBe
        Set(Vector("e", "f", "g"), Vector("1", "2", "3"), Vector("4", "5", "6"))
    }
    "support skipping header" in {
      val file = getClass.getResource("/io/eels/component/csv/csvtest.csv").toURI()
      val path = Paths.get(file)
      CsvSource(path).withHeader(Header.None).toDataStream().toSet.map(_.values) shouldBe
        Set(Vector("a", "b", "c"), Vector("e", "f", "g"), Vector("1", "2", "3"), Vector("4", "5", "6"))
    }
    "support delimiters" in {
      val file = getClass.getResource("/io/eels/component/csv/psv.psv").toURI()
      val path = Paths.get(file)
      CsvSource(path).withDelimiter('|').toDataStream().collect.map(_.values).toSet shouldBe
        Set(Vector("e", "f", "g"))
      CsvSource(path).withDelimiter('|').withHeader(Header.None).toDataStream().toSet.map(_.values) shouldBe
        Set(Vector("a", "b", "c"), Vector("e", "f", "g"))
    }
    "support comments for headers" in {
      val file = getClass.getResource("/io/eels/component/csv/comments.csv").toURI()
      val path = Paths.get(file)
      CsvSource(path).withHeader(Header.FirstComment).schema shouldBe StructType(
        Field("a", StringType, true),
        Field("b", StringType, true),
        Field("c", StringType, true)
      )
      CsvSource(path).withHeader(Header.FirstComment).toDataStream().toSet.map(_.values) shouldBe
        Set(Vector("1", "2", "3"), Vector("e", "f", "g"), Vector("4", "5", "6"))
    }
    "terminate if asking for first comment but no comments" in {
      val file = getClass.getResource("/io/eels/component/csv/csvtest.csv").toURI()
      val path = Paths.get(file)
      CsvSource(path).withHeader(Header.FirstComment).schema shouldBe StructType(
        Field("", StringType, true)
      )
    }
    "support skipping corrupt rows" ignore {
      val file = getClass.getResource("/io/eels/component/csv/corrupt.csv").toURI()
      val path = Paths.get(file)
      CsvSource(path).withHeader(Header.FirstRow).toDataStream().toVector.map(_.values) shouldBe
        Vector(Vector("1", "2", "3"))
    }
  }
}
