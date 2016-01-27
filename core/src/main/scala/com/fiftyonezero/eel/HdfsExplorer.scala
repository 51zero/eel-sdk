package com.fiftyonezero.eel

import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

class HdfsExplorer {
  def findFiles(path: Path, fs: FileSystem): Seq[LocatedFileStatus] = HdfsIterator(fs.listFiles(path, true)).toList
}

object HdfsIterator {
  def apply(iterator: RemoteIterator[LocatedFileStatus]): Iterator[LocatedFileStatus] = new Iterator[LocatedFileStatus] {
    override def hasNext: Boolean = iterator.hasNext
    override def next(): LocatedFileStatus = iterator.next()
  }
}