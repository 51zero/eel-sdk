package io.eels.component.hdfs

import io.eels.FilePattern
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission

case class HdfsSource(pattern: FilePattern)(implicit fs: FileSystem) {

  def permissions(): Vector[(Path, FsPermission)] = pattern.toPaths().map(fs.getFileStatus)
    .map(status => (status.getPath, status.getPermission)).toVector

}

object HdfsSource {
  def apply(path: String)(implicit fs: FileSystem): HdfsSource = apply(FilePattern(path))
  def apply(path: Path)(implicit fs: FileSystem): HdfsSource = HdfsSource(FilePattern(path))
}
