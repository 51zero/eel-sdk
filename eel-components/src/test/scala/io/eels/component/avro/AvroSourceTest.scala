package io.eels.component.avro

import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import io.eels.schema.{Field, StructType}
import org.apache.avro.util.Utf8
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.{Matchers, WordSpec}

class AvroSourceTest extends WordSpec with Matchers {

  private implicit val conf = new Configuration()
  private implicit val fs = FileSystem.get(new Configuration())

  "AvroSource" should {
    "read schema" in {
      val people = AvroSource(Paths.get(getClass.getResource("/test.avro").getFile).toAbsolutePath)
      people.schema shouldBe StructType(Field("name", nullable = false), Field("job", nullable = false), Field("location", nullable = false))
    }
    "read strings as java.lang.String when eel.avro.java.string is true" in {
      System.setProperty("eel.avro.java.string", "true")
      ConfigFactory.invalidateCaches()
      val people = AvroSource(Paths.get(getClass.getResource("/test.avro").getFile).toAbsolutePath).toFrame().toSet
      people.map(_.values) shouldBe Set(
        List("clint eastwood", "actor", "carmel"),
        List("elton john", "musician", "pinner"),
        List("issac newton", "scientist", "heaven")
      )
      System.setProperty("eel.avro.java.string", "false")
      ConfigFactory.invalidateCaches()
    }
    "read strings as utf8 when eel.avro.java.string is false" in {
      System.setProperty("eel.avro.java.string", "false")
      ConfigFactory.invalidateCaches()
      val people = AvroSource(Paths.get(getClass.getResource("/test.avro").getFile).toAbsolutePath).toFrame().toSet
      people.map(_.values) shouldBe Set(
        List(new Utf8("clint eastwood"), new Utf8("actor"), new Utf8("carmel")),
        List(new Utf8("elton john"), new Utf8("musician"), new Utf8("pinner")),
        List(new Utf8("issac newton"), new Utf8("scientist"), new Utf8("heaven"))
      )
      System.setProperty("eel.avro.java.string", "true")
      ConfigFactory.invalidateCaches()
    }
  }
}