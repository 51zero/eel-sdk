package io.eels.component.hbase

import org.apache.hadoop.hbase.client.Scan

/**
  * Helper to build a HBase scanner based on the settings of an HBaseSource instance
  */
object HbaseScanner {

  def apply(hbaseSource: HbaseSource)(implicit serializer: HbaseSerializer): Scan = {
    val scan = new Scan
    val keyField = hbaseSource.schema.fields.find(_.key).getOrElse(sys.error("HBase requires a single column to be define as a key"))
    hbaseSource.cacheBlocks.map(scan.setCacheBlocks)
    hbaseSource.caching.map(scan.setCaching)
    hbaseSource.batch.map(scan.setBatch)
    hbaseSource.startKey.map(startKey => scan.withStartRow(copy(serializer.toBytes(startKey, keyField.name, keyField.dataType))))
    hbaseSource.stopKey.map { key =>
      val stopKey = copy(serializer.toBytes(key, keyField.name, keyField.dataType))
      // If the stop key is marked as inclusive then increment the last byte by one - not fully tested
      if (hbaseSource.stopKeyInclusive) {
        val lastByteIncremented = (stopKey.last.toShort + 1).toByte
        stopKey(stopKey.length - 1) = if (lastByteIncremented > stopKey.last) lastByteIncremented else stopKey.last
      }
      scan.withStopRow(stopKey)
    }
    hbaseSource.consistency.map(scan.setConsistency)
    hbaseSource.isolationLevel.map(scan.setIsolationLevel)
    hbaseSource.timeRange.map(t => scan.setTimeRange(t._1, t._2))
    hbaseSource.timeStamp.map(scan.setTimeStamp)
    hbaseSource.maxVersions.map(scan.setMaxVersions)
    hbaseSource.maxResultsPerColumnFamily.map(scan.setMaxResultsPerColumnFamily)
    hbaseSource.rowOffsetPerColumnFamily.map(scan.setRowOffsetPerColumnFamily)
    hbaseSource.maxResultSize.map(scan.setMaxResultSize)
    hbaseSource.reverseScan.map(scan.setReversed)
    hbaseSource.allowPartialResults.map(scan.setAllowPartialResults)
    hbaseSource.loadColumnFamiliesOnDemand.map(scan.setLoadColumnFamiliesOnDemand)
    hbaseSource.returnDeletedRows.map(scan.setRaw)
    hbaseSource.identifier.map(scan.setId)
    hbaseSource.rowPrefixFilter.map(scan.setRowPrefixFilter)

    // Setup predicate push downs
    hbaseSource.filterList.foreach(scan.setFilter)

    // Set up column projection schema
    hbaseSource.schema.fields
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
