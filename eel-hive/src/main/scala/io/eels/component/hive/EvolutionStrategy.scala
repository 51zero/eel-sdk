package io.eels.component.hive

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
  * A strategy that determines how a hive metastore schema is evolved for a
  * given target schema.
  *
  * For example, a strategy may choose to alter the hive table to add any missing columns.
  * Or it may choose to abort a write by throwing an exception.
  * Or it may choose to leave the schema as is and drop the columns from the input rows.
  */
trait EvolutionStrategy {
  def evolve(dbName: String,
             tableName: String,
             metastoreSchema: StructType,
             targetSchema: StructType,
             client: IMetaStoreClient): Unit
}

object NoopEvolutionStrategy extends EvolutionStrategy {
  override def evolve(dbName: String,
                      tableName: String,
                      metastoreSchema: StructType,
                      targetSchema: StructType,
                      client: IMetaStoreClient): Unit = ()
}

// will error if the target schema does not match the input schema
object LockedEvolutionStrategy extends EvolutionStrategy {
  override def evolve(dbName: String,
                      tableName: String,
                      metastoreSchema: StructType,
                      targetSchema: StructType,
                      client: IMetaStoreClient): Unit = {
    assert(
      metastoreSchema.fields.map(field => field.name -> field.dataType) == targetSchema.fields.map(field => field.name -> field.dataType),
      s"Input schema $targetSchema is not compatible with the metastore schema $metastoreSchema. If you wish eel-sdk to automatically evolve the target schema (where possible) then set evolutionStrategy=AdditionEvolutionStrategy on the HiveSink"
    )
  }
}

/**
  * The AdditionEvolutionStrategy will add any missing fields to the schema in the hive metastore.
  * It will not check that any existing fields are of the same type as in the metastore.
  * The new fields cannot be added as partition fields as the table will already have been created.
  */
object AdditionEvolutionStrategy extends EvolutionStrategy with Logging {

  override def evolve(dbName: String,
                      tableName: String,
                      metastoreSchema: StructType,
                      targetSchema: StructType,
                      client: IMetaStoreClient): Unit = client.synchronized {
    val missing = targetSchema.fields.filterNot(field => metastoreSchema.fieldNames().contains(field.name))
    if (missing.nonEmpty) {
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
}
