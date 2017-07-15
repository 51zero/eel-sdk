package io.eels.component.hive

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
  * A strategy that determines how a hive metastore schema is evolved for a
  * given target schema.
  *
  * For example, a strategy may choose to alter the table to add any missing columns. It may
  * choose to abort a write by throwing an exception. Or it may choose to leave the schema as is.
  */
trait EvolutionStrategy {
  def evolve(dbName: String,
             tableName: String,
             metastoreSchema: StructType,
             targetSchema: StructType,
             client: IMetaStoreClient): Unit
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
object DefaultEvolutionStrategy extends EvolutionStrategy with Logging {

  override def evolve(dbName: String,
                      tableName: String,
                      metastoreSchema: StructType,
                      targetSchema: StructType,
                      client: IMetaStoreClient): Unit = {
    val missing = targetSchema.fields.filterNot(field => metastoreSchema.fieldNames().contains(field.name))
    logger.debug("Hive metastore is missing the following fields: " + missing.mkString(", "))
    val table = client.getTable(dbName, tableName)
    val cols = table.getSd.getCols
    missing.foreach { field =>
      logger.info(s"Adding new column to hive table [$field]")
      cols.add(HiveSchemaFns.toHiveField(field))
    }
    table.getSd.setCols(cols)
    client.alter_table(dbName, tableName, table)
  }
}
