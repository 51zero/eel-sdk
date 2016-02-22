package io.eels.component.csv

import java.nio.charset.Charset
import java.nio.file.Files

import io.eels.Frame
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class CsvSinkTest extends WordSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  val frame = Frame(
    List("a", "b", "c", "d"),
    List("1", "2", "3", "4"),
    List("5", "6", "7", "8")
  )

  "CsvSink" should {
    "write csv data" in {
      val temp = Files.createTempFile("csvsink", ".test")
      frame.to(CsvSink(temp))
      val result = Files.readAllLines(temp, Charset.defaultCharset).asScala.toSet
      result shouldBe Set("1,2,3,4", "5,6,7,8")
    }
    "support setting delimiter" in {
      val temp = Files.createTempFile("csvsink", ".test")
      frame.to(CsvSink(temp, CsvSinkProps(delimiter = '>')))
      val result = Files.readAllLines(temp, Charset.defaultCharset).asScala.toSet
      result shouldBe Set("1>2>3>4", "5>6>7>8")
    }
  }
}
