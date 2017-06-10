package io.eels.component.avro

import io.eels.Frame
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class AvroSinkTest extends WordSpec with Matchers {

  private implicit val conf = new Configuration()
  private implicit val fs = FileSystem.get(new Configuration())

  private val frame = Frame.fromValues(
    StructType("name", "job", "location"),
    List("clint eastwood", "actor", "carmel"),
    List("elton john", "musician", "pinner"),
    List("issac newton", "scientist", "heaven")
  )

  "AvroSink" should {
    "write to avro" in {
      val path = new Path("avro.test")
      fs.delete(path, false)
      frame.to(AvroSink(path))
      fs.delete(path, false)
    }
    "support overwrite option" in {
      val path = new Path("overwrite_test", ".avro")
      fs.delete(path, false)
      frame.to(AvroSink(path))
      frame.to(AvroSink(path).withOverwrite(true))
      fs.delete(path, false)
    }
  }
}