package io.eels.component.hive

import com.sksamuel.scalax.Logging
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{InternalRow, SourceReader}
import io.eels.component.hive.dialect.{AvroHiveDialect, OrcHiveDialect, ParquetHiveDialect, TextHiveDialect}
import io.eels.util.Logging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.api.Table



object HiveDialect extends Logging {


}

