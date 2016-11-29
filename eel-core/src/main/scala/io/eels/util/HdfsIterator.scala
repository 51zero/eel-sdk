package io.eels.util

import org.apache.hadoop.fs.{LocatedFileStatus, RemoteIterator}

object HdfsIterator {
  def apply(iterator: RemoteIterator[LocatedFileStatus]): Iterator[LocatedFileStatus] = new Iterator[LocatedFileStatus] {
    override def hasNext(): Boolean = iterator.hasNext()
    override def next(): LocatedFileStatus = iterator.next()
  }
}