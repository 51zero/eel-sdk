package io.eels

import com.sksamuel.exts.Logging
import io.eels.util.{HdfsIterator, PathIterator}
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}

object HdfsOps extends Logging {

  def makePathVisible(path: Path)(implicit fs: FileSystem): Unit = {
    if (path.getName.startsWith(".")) {
      logger.info(s"Making $path visible by stripping leading .")
      val dest = new Path(path.getParent, path.getName.drop(1))
      fs.rename(path, dest)
    }
  }

  def findFiles(path: Path, recursive: Boolean, fs: FileSystem): Iterator[LocatedFileStatus] = {
    HdfsIterator(fs.listFiles(path, recursive))
  }

  def mkdirsp(path: Path, fs: FileSystem): Boolean = PathIterator(path).forall(fs.mkdirs)
}