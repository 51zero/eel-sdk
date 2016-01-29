package io.eels.component.parquet

import java.io.File

import io.eels.{Column, FrameSchema, Row}
import org.scalatest.{Matchers, WordSpec}

class ParquetSourceTest extends WordSpec with Matchers {

  val personFile = new File(getClass.getResource("/parquetfiles/person.pq").getFile)
  val resourcesDir = personFile.getParentFile.getAbsolutePath

  "ParquetSource" should {
    "read schema" in {
      val people = ParquetSource(personFile.getAbsolutePath)
      people.schema shouldBe FrameSchema(Seq(Column("name"), Column("job"), Column("location")))
    }
    "read parquet files" in {
      val people = ParquetSource(personFile.getAbsolutePath).toList
      people shouldBe List(
        Row(Seq(Column("name"), Column("job"), Column("location")), Seq("clint eastwood", "actor", "carmel")),
        Row(Seq(Column("name"), Column("job"), Column("location")), Seq("elton john", "musician", "pinner"))
      )
    }
    "read multiple parquet files using file expansion" in {
      val people = ParquetSource(resourcesDir + "/*").toList
      people shouldBe List(
        Row(Seq(Column("name"), Column("job"), Column("location")), Seq("clint eastwood", "actor", "carmel")),
        Row(Seq(Column("name"), Column("job"), Column("location")), Seq("elton john", "musician", "pinner")),
        Row(Seq(Column("name"), Column("job"), Column("location")), Seq("clint eastwood", "actor", "carmel")),
        Row(Seq(Column("name"), Column("job"), Column("location")), Seq("elton john", "musician", "pinner"))
      )
    }
  }
}

