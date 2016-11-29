package io.eels.util

import org.apache.hadoop.fs.Path

// returns an iterator over the parts of a Hadoop Path
object PathIterator {
  def apply(path: Path): Iterator[Path] = Iterator.iterate(path)(_.getParent).takeWhile(_ != null)
}
