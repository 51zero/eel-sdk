package io.eels.component.hive

import com.sksamuel.exts.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient

case class Compactor(dbname: String, tablename: String)
                    (implicit fs: FileSystem, conf: Configuration,client: IMetaStoreClient) extends Logging {

  /**
    * Compacts the files in the given hive datbase.table
    * Returns the original number of files.
    * Automatically handles partitions.
    */
  def compactTo(targetFileCount: Int): Int = {
    val originalPaths = HiveTable(dbname, tablename).paths(false, false)
    HiveSource(dbname, tablename).toDataStream().to(HiveSink(dbname, tablename), targetFileCount)
    originalPaths.foreach(fs.delete(_, false))
    originalPaths.size
  }
}
