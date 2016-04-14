package io.eels

import org.apache.hadoop.fs.Path
import java.util.stream.Stream

object PathIterator {
  operator fun invoke(path: Path): Iterator<Path> = Stream.iterate(path, { it.parent }).nullTerminatedIterator()
}