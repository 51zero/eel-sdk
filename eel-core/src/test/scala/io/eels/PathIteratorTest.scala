package io.eels

import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class PathIteratorTest extends WordSpec with Matchers {
  "PathIterator" should {
    "return all non-null parent paths" in {
      val path = new Path("/some/path/for/fun")
      PathIterator(path).toSet shouldBe Set(new Path("/some/path/for/fun"), new Path("/some/path/for"), new Path("/some/path"), new Path("/some"), new Path("/"))
    }
  }
}