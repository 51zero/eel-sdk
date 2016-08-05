package io.eels

import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.RemoteIterator

object HdfsIterator {
  def apply(iterator: RemoteIterator[LocatedFileStatus]): Iterator[LocatedFileStatus] = new Iterator[LocatedFileStatus] {
    override def hasNext(): Boolean = iterator.hasNext()
    override def next(): LocatedFileStatus = iterator.next()
  }
}