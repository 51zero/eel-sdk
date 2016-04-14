package io.eels

import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.RemoteIterator

object HdfsIterator {
  operator fun invoke(iterator: RemoteIterator<LocatedFileStatus>): Iterator<LocatedFileStatus> = object : Iterator<LocatedFileStatus> {
    override fun hasNext(): Boolean = iterator.hasNext()
    override fun next(): LocatedFileStatus = iterator.next()
  }
}