package io.eels.component.avro

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import io.eels.{Column, Schema}
import org.apache.avro.util.Utf8
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class AvroSourceTest extends WordSpec with Matchers with OneInstancePerTest {

  import scala.concurrent.ExecutionContext.Implicits.global

  "AvroSource" should {
    "read schema" in {
      val people = AvroSource(Paths.get(new File(getClass.getResource("/test.avro").getFile).getAbsolutePath))
      people.schema shouldBe Schema(List(Column("name"), Column("job"), Column("location")))
    }
    "read strings as java.lang.String when eel.avro.java.string is true" in {
      sys.props.put("eel.avro.java.string", "true")
      ConfigFactory.invalidateCaches()
      val people = AvroSource(Paths.get(new File(getClass.getResource("/test.avro").getFile).getAbsolutePath)).toSet
      people.map(_.values) shouldBe Set(
        List("clint eastwood", "actor", "carmel"),
        List("elton john", "musician", "pinner"),
        List("issac newton", "scientist", "heaven")
      )
    }
    "read strings as utf8 when eel.avro.java.string is false" in {
      sys.props.put("eel.avro.java.string", "false")
      ConfigFactory.invalidateCaches()
      val people = AvroSource(Paths.get(new File(getClass.getResource("/test.avro").getFile).getAbsolutePath)).toSet
      people.map(_.values) shouldBe Set(
        List(new Utf8("clint eastwood"), new Utf8("actor"), new Utf8("carmel")),
        List(new Utf8("elton john"), new Utf8("musician"), new Utf8("pinner")),
        List(new Utf8("issac newton"), new Utf8("scientist"), new Utf8("heaven"))
      )
    }
  }
}