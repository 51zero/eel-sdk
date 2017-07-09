package io.eels.util

import org.apache.hadoop.fs.{FileSystem, Path}

object HdfsMkdir {
  def apply(path: Path, inheritPermissionsDefault: Boolean)(implicit fs: FileSystem): Unit = {
    if (!fs.exists(path)) {
      // iterate through the parents until we hit a parent that exists, then take that, which will give
      // us the first folder that exists
      val parent = Iterator.iterate(path)(_.getParent).dropWhile(false == fs.exists(_)).take(1).toList.head
      // using the folder that exists, get its permissions
      val permission = fs.getFileStatus(parent).getPermission
      fs.create(path, false)
      if (inheritPermissionsDefault)
        fs.setPermission(path, permission)
    }
  }
}