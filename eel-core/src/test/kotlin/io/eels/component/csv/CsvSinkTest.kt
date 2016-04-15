package io.eels.component.csv

import java.nio.charset.Charset
import java.nio.file.Files

import io.kotlintest.specs.WordSpec

class CsvSinkTest : WordSpec() {

  //  init {
  //
  //    val frame = Frame(
  //        arrayOf("a", "b", "c", "d"),
  //        arrayOf("1", "2", "3", "4"),
  //        arrayOf("5", "6", "7", "8")
  //    )
  //
  //    "CsvSink" should {
  //      "write csv data" with {
  //        val temp = Files.createTempFile("csvsink", ".test")
  //        frame.to(CsvSink(temp))
  //        val result = Files.readAllLines(temp, Charset.defaultCharset()).toSet()
  //        result shouldBe setOf("1,2,3,4", "5,6,7,8")
  //      }
  //      "support setting delimiter" with {
  //        val temp = Files.createTempFile("csvsink", ".test")
  //        frame.to(CsvSink(temp, format = CsvFormat().copy(delimiter = '>')))
  //        val result = Files.readAllLines(temp, Charset.defaultCharset()).toSet()
  //        result shouldBe setOf("1>2>3>4", "5>6>7>8")
  //      }
  //    }
  //  }
}
