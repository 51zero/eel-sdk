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
      people.schema shouldBe FrameSchema(List(Column("name"), Column("job"), Column("location")))
    }
    "read parquet files" in {
      val people = ParquetSource(personFile.getAbsolutePath).toList.runConcurrent(2)
      people shouldBe List(
        Row(List(Column("name"), Column("job"), Column("location")), List("clint eastwood", "actor", "carmel")),
        Row(List(Column("name"), Column("job"), Column("location")), List("elton john", "musician", "pinner"))
      )
    }
    "read multiple parquet files using file expansion" in {
      val people = ParquetSource(resourcesDir + "/*").toList.runConcurrent(2)
      people shouldBe List(
        Row(List(Column("name"), Column("job"), Column("location")), List("clint eastwood", "actor", "carmel")),
        Row(List(Column("name"), Column("job"), Column("location")), List("elton john", "musician", "pinner")),
        Row(List(Column("name"), Column("job"), Column("location")), List("clint eastwood", "actor", "carmel")),
        Row(List(Column("name"), Column("job"), Column("location")), List("elton john", "musician", "pinner"))
      )
    }
  }
}

