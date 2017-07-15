package io.eels.component.hive

import io.eels.schema.StructType
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
  * Accepts a metastore schema and returns the schema that should actually be persisted to disk.
  * This allows us to determine if some data is not written, for example in parquet files
  * it is common to skip writing out partition data, since that data is present in the metastore.
  */
trait OutputSchemaStrategy {
  def resolve(schema: StructType, partitionKeys: Seq[String], client: IMetaStoreClient): StructType
}

/**
  * This strategy will drop partition columns from the schema
  * so that they not written out to the files.
  */
object SkipPartitionsOutputSchemaStrategy extends OutputSchemaStrategy {

  def resolve(schema: StructType, partitionKeys: Seq[String], client: IMetaStoreClient): StructType = {
    if (partitionKeys.isEmpty) schema
    else
      partitionKeys.foldLeft(schema) { (schema, name) =>
        schema.removeField(name, caseSensitive = false)
      }
  }
}