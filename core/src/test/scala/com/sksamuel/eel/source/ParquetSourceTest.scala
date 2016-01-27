package com.sksamuel.eel.source

import java.io.File

import com.sksamuel.eel.sink.{Column, Row}
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class ParquetSourceTest extends WordSpec with Matchers {

  "ParquetSource" should {
    "read parquet files" in {
      val people = ParquetSource(new Path(new File(getClass.getResource("/person.pq").getFile).getAbsolutePath)).toList
      people shouldBe List(
        Row(Seq(Column("name"), Column("job"), Column("location")), Seq("clint eastwood", "actor", "carmel")),
        Row(Seq(Column("name"), Column("job"), Column("location")), Seq("elton john", "musician", "pinner"))
      )
    }
  }
}

