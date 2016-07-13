package io.eels.component.avro

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import io.eels.schema.Field
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import org.apache.avro.util.Utf8

class AvroSourceTest : WordSpec() {
  init {

    "AvroSource" should {
      "read schema"  {
        val people = AvroSource(Paths.get(File(javaClass.getResource("/test.avro").file).absolutePath))
        people.schema() shouldBe Schema(Field("name", nullable = false), Field("job", nullable = false), Field("location", nullable = false))
      }
      "read strings as java.lang.String when eel.avro.java.string is true"  {
        System.setProperty("eel.avro.java.string", "true")
        ConfigFactory.invalidateCaches()
        val people = AvroSource(Paths.get(File(javaClass.getResource("/test.avro").file).absolutePath)).toFrame(1).toSet()
        people.map { it.values }.toSet() shouldBe setOf(
            listOf("clint eastwood", "actor", "carmel"),
            listOf("elton john", "musician", "pinner"),
            listOf("issac newton", "scientist", "heaven")
        )
        System.setProperty("eel.avro.java.string", "true")
        ConfigFactory.invalidateCaches()
      }
      "read strings as utf8 when eel.avro.java.string is false"  {
        System.setProperty("eel.avro.java.string", "false")
        ConfigFactory.invalidateCaches()
        val people = AvroSource(Paths.get(File(javaClass.getResource("/test.avro").file).absolutePath)).toFrame(1).toSet()
        people.map { it.values }.toSet() shouldBe setOf(
            listOf(Utf8("clint eastwood"), Utf8("actor"), Utf8("carmel")),
            listOf(Utf8("elton john"), Utf8("musician"), Utf8("pinner")),
            listOf(Utf8("issac newton"), Utf8("scientist"), Utf8("heaven"))
        )
        System.setProperty("eel.avro.java.string", "true")
        ConfigFactory.invalidateCaches()
      }
    }
  }
}