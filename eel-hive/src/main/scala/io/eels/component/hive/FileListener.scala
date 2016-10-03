package io.eels.component.hive

import org.apache.hadoop.fs.Path

object FileListener {
  val noop = new FileListener {
    override def onFileCreated(path: Path): Unit = ()
  }
}


trait FileListener {
  def onFileCreated(path: Path)
}
