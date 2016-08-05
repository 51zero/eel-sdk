package io.eels

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path

object HdfsOps {

  def findFiles(path: Path, recursive: Boolean, fs: FileSystem): Iterator[LocatedFileStatus] = {
    HdfsIterator(fs.listFiles(path, recursive))
  }

  def mkdirsp(path: Path, fs: FileSystem): Boolean = PathIterator(path).forall(fs.mkdirs)
}