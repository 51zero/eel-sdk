package io.eels.component.sequence

import io.eels.Frame
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, IntWritable, SequenceFile}
import org.scalatest.{Matchers, WordSpec}

class SequenceSinkTest extends WordSpec with Matchers {

  val frame = Frame.fromValues(
    StructType("a", "b", "c", "d"),
    List("1", "2", "3", "4"),
    List("5", "6", "7", "8")
  )

  "SequenceSink" should {
    "write sequence files" in {

      implicit val conf = new Configuration
      implicit val fs = FileSystem.get(conf)

      val path = new Path("seqsink.seq")
      if (fs.exists(path))
        fs.delete(path, true)

      frame.to(SequenceSink(path))

      val reader = new SequenceFile.Reader(new Configuration, SequenceFile.Reader.file(path))

      val k = new IntWritable
      val v = new BytesWritable

      val set = for (_ <- 1 to 3) yield {
        reader.next(k, v)
        new String(v.copyBytes)
      }

      set.toSet shouldBe Set(
        "a,b,c,d",
        "1,2,3,4",
        "5,6,7,8"
      )

      reader.close()

      fs.delete(path, true)
    }
  }
}
