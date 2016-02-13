package io.eels

import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}

object HdfsOps {

  def findFiles(path: Path, fs: FileSystem): Seq[LocatedFileStatus] = HdfsIterator(fs.listFiles(path, true)).toList

  def mkdirsp(path: Path)(implicit fs: FileSystem): Boolean = PathIterator(path).forall(fs.mkdirs)
}

