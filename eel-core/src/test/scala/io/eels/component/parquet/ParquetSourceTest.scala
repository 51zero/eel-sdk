package io.eels.component.parquet

import java.io.File

import io.eels.{Column, FrameSchema}
import org.scalatest.{Matchers, WordSpec}

class ParquetSourceTest extends WordSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  val personFile = new File(getClass.getResource("/parquetfiles/person.pq").getFile)
  val resourcesDir = personFile.getParentFile.getAbsolutePath

  "ParquetSource" should {
    "read schema" in {
      val people = ParquetSource(personFile.getAbsolutePath)
      people.schema shouldBe FrameSchema(List(Column("name"), Column("job"), Column("location")))
    }
    "read parquet files" in {
      val people = ParquetSource(personFile.getAbsolutePath).toSet.map(_.values.map(_.toString))
      people shouldBe Set(
        List("clint eastwood", "actor", "carmel"),
        List("elton john", "musician", "pinner")
      )
    }
    "read multiple parquet files using file expansion" in {
      val people = ParquetSource(resourcesDir + "/*").toSet.map(_.values.map(_.toString))
      people shouldBe Set(
        List("clint eastwood", "actor", "carmel"),
        List("elton john", "musician", "pinner"),
        List("clint eastwood", "actor", "carmel"),
        List("elton john", "musician", "pinner")
      )
    }
  }
}

