package io.eels.cli

import java.io.{ByteArrayOutputStream, PrintStream}

import org.scalatest.{Matchers, WordSpec}

class ShowSchemaMainTest extends WordSpec with Matchers {

  "SchemaMain" should {
    "display schema for specified avro source" in {
      val baos = new ByteArrayOutputStream
      val out = new PrintStream(baos)
      ShowSchemaMain(Seq("--source", "avro:" + getClass.getResource("/test.avro").getFile), out)
      new String(baos.toByteArray).trim shouldBe """{"type":"record","name":"row","namespace":"namespace","fields":[{"name":"name","type":"string"},{"name":"job","type":"string"},{"name":"location","type":"string"}]}"""
    }
  }
}
