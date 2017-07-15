package io.eels.component.hive

import io.eels.Row
import io.eels.schema.StructType
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
  * A strategy that determines how a hive metastore schema is evolved for a
  * given target schema.
  *
  * For example, a strategy may choose to alter the table to add any missing columns. It may
  * choose to abort a write by throwing an exception. Or it may choose to leave the schema as is,
  * and then strip the superfluous columns from the rows.
  */
trait EvolutionStrategy {

  /**
    * Given details about a table, evolve the schema to match.
    */
  def evolve(dbName: String,
             tableName: String,
             metastoreSchema: StructType,
             targetSchema: StructType,
             client: IMetaStoreClient): Unit

  /**
    * Given a row that is to be persisted to hive, returns a row that matches the required schema.
    */
  def align(row: Row, metastoreSchema: StructType): Row
}

/**
  * The DefaultEvolutionStrategy will add missing fields to the schema in the hive metastore.
  * It will not check that any existing fields are of the same type as in the metastore.
  *
  * Any fields that are present in the input row, but not present in the metastore will be silently dropped.
  *
  * Any fields that are present in the metastore, but not in the row will be padded with the fields
  * default value, or null (if the field is nullable). If the field has no default and is not nullable
  * then an exception will be thrown.
  */
object DefaultEvolutionStrategy extends EvolutionStrategy {

  override def evolve(dbName: String,
                      tableName: String,
                      metastoreSchema: StructType,
                      targetSchema: StructType,
                      client: IMetaStoreClient): Unit = {
    val missing = targetSchema.fields.filterNot(field => metastoreSchema.fieldNames().contains(field.name))
    val table = client.getTable(dbName, tableName)
    val cols = table.getSd.getCols
    missing.foreach { field =>
      cols.add(HiveSchemaFns.toHiveField(field))
    }
    table.getSd.setCols(cols)
    client.alter_table(dbName, tableName, table)
  }

  def align(row: Row, metastoreSchema: StructType): Row = {
    val map = row.schema.fieldNames().zip(row.values).toMap
    // for each field in the metastore, get the field from the input row, and use that
    // if the input map does not have it, then pad it with a default or null
    val values = metastoreSchema.fields.map { field =>
      map.getOrElse(field.name, field.default)
    }
    Row(metastoreSchema, values)
  }
}
