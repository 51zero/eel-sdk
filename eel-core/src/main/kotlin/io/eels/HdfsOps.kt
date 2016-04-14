package io.eels

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path

object HdfsOps {

  fun findFiles(path: Path, recursive: Boolean, fs: FileSystem): Iterator<LocatedFileStatus> {
    return HdfsIterator(fs.listFiles(path, recursive))
  }

  fun mkdirsp(path: Path, fs: FileSystem): Boolean = PathIterator(path).asSequence().all { fs.mkdirs(it) }
}