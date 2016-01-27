package com.sksamuel.eel.source

import java.nio.file.Paths

import com.sksamuel.eel.{Column, FrameSchema}
import org.scalatest.{Matchers, WordSpec}

class CsvSourceTest extends WordSpec with Matchers {

  "CsvSource" should {
    "read schema" in {
      val path = Paths.get(getClass.getResource("/csvtest.csv").getFile)
      CsvSource(path).schema shouldBe FrameSchema(List(Column("a"), Column("b"), Column("c")))
    }
    "read from path" in {
      val path = Paths.get(getClass.getResource("/csvtest.csv").getFile)
      CsvSource(path).size shouldBe 3
    }
  }
}
