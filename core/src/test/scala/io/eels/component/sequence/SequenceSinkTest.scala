package io.eels.component.sequence

import io.eels.{Column, Frame, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, IntWritable, SequenceFile}
import org.scalatest.{Matchers, WordSpec}

class SequenceSinkTest extends WordSpec with Matchers {

  val columns = List(Column("a"), Column("b"), Column("c"), Column("d"))
  val frame = Frame(Row(columns, List("1", "2", "3", "4")), Row(columns, List("5", "6", "7", "8")))

  "SequenceSink" should {
    "write sequence files" in {

      implicit val fs = FileSystem.get(new Configuration)

      val path = new Path("test.seq")
      if (fs.exists(path))
        fs.delete(path, true)

      frame.to(SequenceSink(path)).runConcurrent(3)

      val reader = new SequenceFile.Reader(new Configuration, SequenceFile.Reader.file(path))

      val k = new IntWritable
      val v = new BytesWritable

      reader.next(k, v)
      new String(v.copyBytes) shouldBe "a,b,c,d"

      reader.next(k, v)
      new String(v.copyBytes) shouldBe "1,2,3,4"

      reader.next(k, v)
      new String(v.copyBytes) shouldBe "5,6,7,8"

      reader.close()

      fs.delete(path, true)
    }
  }
}
