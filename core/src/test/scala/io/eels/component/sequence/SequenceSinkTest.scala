package io.eels.component.sequence

import java.nio.file.{Files, Paths}

import io.eels.sink.SequenceSink
import io.eels.{Column, Frame, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, BytesWritable, SequenceFile}
import org.scalatest.{Matchers, WordSpec}

class SequenceSinkTest extends WordSpec with Matchers {

  val columns = Seq(Column("a"), Column("b"), Column("c"), Column("d"))
  val frame = Frame(Row(columns, Seq("1", "2", "3", "4")), Row(columns, Seq("5", "6", "7", "8")))

  "SequenceSink" should {
    "write sequence files" in {
      val path = new Path("test")
      frame.to(SequenceSink(path))

      Files.readAllBytes(Paths.get(path.toString))

      val reader = new SequenceFile.Reader(new Configuration, SequenceFile.Reader.file(path))
      val w = new BytesWritable
      reader.next(new IntWritable(0))
      reader.getCurrentValue(w)
      new String(w.copyBytes()) shouldBe "1,2,3,4"
      reader.next(new IntWritable(1))
      reader.getCurrentValue(w)
      new String(w.copyBytes()) shouldBe "5,6,7,8"
      reader.close()
    }
  }
}
