package io.eels.component.avro

import java.io.ByteArrayOutputStream
import java.nio.file.Files

import io.eels.Frame
import io.eels.schema.StructType
import org.scalatest.{Matchers, WordSpec}

class AvroSinkTest extends WordSpec with Matchers {

  val frame = Frame.fromValues(
    StructType("name", "job", "location"),
    List("clint eastwood", "actor", "carmel"),
    List("elton john", "musician", "pinner"),
    List("issac newton", "scientist", "heaven")
  )

  "AvroSink" should {
    "write to avro" in {
      val baos = new ByteArrayOutputStream()
      frame.to(AvroSink(baos))
    }
    "support overwrite option" in {
      val path = Files.createTempFile("overwrite_test", ".avro")
      frame.to(AvroSink(path))
      frame.to(AvroSink(path, true))
      path.toFile.delete()
    }
  }
}
