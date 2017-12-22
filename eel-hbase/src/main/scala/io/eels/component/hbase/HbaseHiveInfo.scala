package io.eels.component.hbase

import io.eels.schema.StructType

case class HbaseHiveInfo(namespace: String, table: String, schema: StructType, hbaseSerializer: HbaseSerializer)
