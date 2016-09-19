package io.eels.component.hdfs

import io.eels.FilePattern
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.{AclEntryScope, AclEntryType, FsAction, FsPermission, AclEntry => HdfsAclEntry}
import scala.collection.JavaConverters._

case class HdfsSource(pattern: FilePattern)(implicit fs: FileSystem) {

  def permissions(): Vector[(Path, FsPermission)] = pattern.toPaths().map(fs.getFileStatus)
    .map(status => (status.getPath, status.getPermission)).toVector

  def setAcl(spec: AclSpec): Unit = {
    pattern.toPaths().foreach { path =>
      val hadoopAclEntries = spec.entries.map { entry =>
        val `type` = entry.`type`.toLowerCase match {
          case "user" => AclEntryType.USER
          case "group" => AclEntryType.GROUP
          case "other" => AclEntryType.OTHER
        }
        new HdfsAclEntry.Builder().setName(entry.name).setPermission(FsAction.getFsAction(entry.action)).setType(`type`).setScope(AclEntryScope.ACCESS).build()
      }
      fs.setAcl(path, hadoopAclEntries.asJava)
    }
  }
}

object HdfsSource {
  def apply(path: String)(implicit fs: FileSystem): HdfsSource = apply(FilePattern(path))
  def apply(path: Path)(implicit fs: FileSystem): HdfsSource = HdfsSource(FilePattern(path))
}
