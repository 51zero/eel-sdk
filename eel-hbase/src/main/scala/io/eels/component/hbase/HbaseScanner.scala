package io.eels.component.hbase

import io.eels.component.hbase.HbaseScanner.copy
import io.eels.schema.StructType
import org.apache.hadoop.hbase.client.Scan

/**
  * Helper to build a HBase scanner based on the settings of an HBaseSource instance
  */
object HbaseScanner {

  def apply(schema: StructType, hbaseSource: HbaseSource)(implicit serializer: HbaseSerializer): Scan = {
    val scan = new Scan
    val keyField = schema.fields.find(_.key).getOrElse(sys.error("HBase requires a single column to be define as a key"))
    hbaseSource.cacheBlocks.filterNot(_ == None).map(scan.setCacheBlocks)
    hbaseSource.caching.filterNot(_ == None).map(scan.setCaching)
    hbaseSource.batch.filterNot(_ == None).map(scan.setBatch)
    hbaseSource.startKey.filterNot(_ == None).map(startKey => scan.withStartRow(copy(serializer.toBytes(startKey, keyField.name, keyField.dataType))))
    hbaseSource.stopKey.map { key =>
      val stopKey = copy(serializer.toBytes(key, keyField.name, keyField.dataType))
      // If the stop key is marked as inclusive then increment the last byte by one - not fully tested
      if (hbaseSource.stopKeyInclusive) {
        val lastByteIncremented = (stopKey.last.toShort + 1).toByte
        stopKey(stopKey.length - 1) = if (lastByteIncremented > stopKey.last) lastByteIncremented else stopKey.last
      }
      scan.withStopRow(stopKey)
    }
    hbaseSource.consistency.filterNot(_ == None).map(scan.setConsistency)
    hbaseSource.isolationLevel.filterNot(_ == None).map(scan.setIsolationLevel)
    hbaseSource.timeRange.filterNot(_ == None).map(t => scan.setTimeRange(t._1, t._2))
    hbaseSource.timeStamp.filterNot(_ == None).map(scan.setTimeStamp)
    hbaseSource.maxVersions.filterNot(_ == None).map(scan.setMaxVersions)
    hbaseSource.maxResultsPerColumnFamily.filterNot(_ == None).map(scan.setMaxResultsPerColumnFamily)
    hbaseSource.rowOffsetPerColumnFamily.filterNot(_ == None).map(scan.setRowOffsetPerColumnFamily)
    hbaseSource.maxResultSize.filterNot(_ == None).map(scan.setMaxResultSize)
    hbaseSource.reverseScan.filterNot(_ == None).map(scan.setReversed)
    hbaseSource.allowPartialResults.filterNot(_ == None).map(scan.setAllowPartialResults)
    hbaseSource.loadColumnFamiliesOnDemand.filterNot(_ == None).map(scan.setLoadColumnFamiliesOnDemand)
    hbaseSource.returnDeletedRows.filterNot(_ == None).map(scan.setRaw)
    hbaseSource.identifier.filterNot(_ == None).map(scan.setId)
    hbaseSource.rowPrefixFilter.filterNot(_ == None).map(scan.setRowPrefixFilter)

    // Setup predicate push downs
    hbaseSource.filterList.filterNot(_ == None).map(scan.setFilter)

    // Set up column projection schema
    schema.fields
      .filter(!_.key)
      .foreach(f => scan.addColumn(f.columnFamily.get.getBytes, f.name.getBytes))

    scan
  }

  private def copy(sourceArray: Array[Byte]): Array[Byte] = {
    val bufferCopy = new Array[Byte](sourceArray.length)
    System.arraycopy(sourceArray, 0, bufferCopy, 0, bufferCopy.length)
    bufferCopy
  }
}
