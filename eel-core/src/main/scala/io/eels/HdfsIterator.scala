package io.eels

import org.apache.hadoop.fs.{Path, LocatedFileStatus, RemoteIterator}

object HdfsIterator {
  def apply(iterator: RemoteIterator[LocatedFileStatus]): Iterator[LocatedFileStatus] = new Iterator[LocatedFileStatus] {
    override def hasNext: Boolean = iterator.hasNext
    override def next(): LocatedFileStatus = iterator.next()
  }
}

object PathIterator {
  def apply(path: Path): Iterator[Path] = {
    Iterator.iterate(path)(_.getParent).takeWhile(_ != null).toList.reverseIterator
  }
}