package io.eels

import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}

object HdfsOps {

  def findFiles(path: Path, recursive: Boolean)(implicit fs: FileSystem): Iterator[LocatedFileStatus] = {
    HdfsIterator(fs.listFiles(path, recursive))
  }

  def mkdirsp(path: Path)(implicit fs: FileSystem): Boolean = PathIterator(path).forall(fs.mkdirs)
}

