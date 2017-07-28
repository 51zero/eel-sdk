package io.eels.component.hive

import org.apache.hadoop.fs.{FileSystem, Path}

trait StagingStrategy2 {

}

trait StagingStrategy {

  /**
    * Return true if you want eel to create output files in a temporary staging area before
    * committing the files into the table location. The commit phase will happen once all writes
    * have been completed into the staging area.
    *
    * Return false if you want to write directly into the table location.
    */
  def staging: Boolean

  /**
    * Return a Path where eel should create the output files.
    *
    * The path should be absolute, and can be created as a child of the table
    * or partition path which is provided as the parameter. Or it can be created
    * anywhere else you choose by just returning a different absolute path.
    *
    * If staging is disabled then return None.
    */
  def stagingDirectory(parent: Path, fs: FileSystem): Option[Path]

  /**
    * Returns true if eel should move written data from the staging directory
    * into the table itself once all the writes have completed.
    *
    * If you wish to manually move data from staging, then return false,
    * and eel will leave the files inside the staging directory.
    */
  def commit: Boolean
}

trait CommitCallback {
  def onCommit(stagingPath: Path, finalPath: Path): Unit
  def onCommitComplete()
}

object DefaultStagingStrategy extends StagingStrategy {
  override def staging: Boolean = true
  override def stagingDirectory(parent: Path, fs: FileSystem): Option[Path] = {
    Iterator.from(1).map(k => new Path(parent, s".eel_staging_$k")).dropWhile(fs.exists).take(1).toList.headOption
  }
  override def commit: Boolean = true
}