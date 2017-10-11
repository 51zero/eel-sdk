package io.eels.component.hive

import com.sksamuel.exts.Logging
import org.apache.hadoop.fs.FileSystem

object Compactor extends Logging {

  /**
    * Compacts the files in the given hive datbase.table
    * Returns the original number of files.
    * Automatically handles partitions.
    */
  def apply(dbname: String, tablename: String, targetFileCount: Int)(implicit fs: FileSystem): Int = {
    val originalPaths = HiveTable(dbname, tablename).paths(false, false)
    HiveSource(dbname, tablename).toDataStream().to(HiveSink(dbname, tablename), targetFileCount)
    originalPaths.foreach(fs.delete(_, false))
    originalPaths.size
  }
}
