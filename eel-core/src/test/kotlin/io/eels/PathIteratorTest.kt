package io.eels

import io.kotlintest.specs.WordSpec
import org.apache.hadoop.fs.Path

class PathIteratorTest : WordSpec() {
  init {
    "PathIterator" should {
      "return all non-null parent paths" {
        val path = Path("/some/path/for/fun")
        PathIterator(path).asSequence().toSet() shouldBe setOf(Path("/some/path/for/fun"), Path("/some/path/for"), Path("/some/path"), Path("/some"), Path("/"))
      }
    }
  }
}