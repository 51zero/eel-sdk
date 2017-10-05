package io.eels.component.hive

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
  * A handler that is invoked with the schema of the source and the
  * existing schema in the metastore.
  *
  * This allows a handler to decide how to handle differences. For instance
  * an implementation may choose to evolve the metastore schema to add missing fields.
  * Another implemention may throw an exception if the schemas are not aligned.
  */
trait MetastoreSchemaHandler {
  def evolve(dbName: String,
             tableName: String,
             metastoreSchema: StructType,
             targetSchema: StructType,
             client: IMetaStoreClient): Unit
}

/**
  * An implementation of MetastoreSchemaHandler that does nothing, this
  * may result in errors downstream if, for example, the input schema does not
  * include all columns and defaults cannot be applied.
  */
object NoopMetastoreSchemaHandler extends MetastoreSchemaHandler {
  override def evolve(dbName: String,
                      tableName: String,
                      metastoreSchema: StructType,
                      targetSchema: StructType,
                      client: IMetaStoreClient): Unit = ()
}

/**
  * An implementation of MetastoreSchemaHandler that requires the input
  * schema to be compatible with the metastore schema. Compatiblity is
  * achieved when all fields in the input schema are already defined
  * in the metastore, with compatible types.
  *
  * With this handler, the input schema is allowed to have extra fields
  * which are not present in the metastore. It is assumed they will be
  * dropped by the alignment strategy.
  *
  * If the schemas are not compatible then an exception is raised.
  */
object RequireCompatibilityMetastoreSchemaHandler extends MetastoreSchemaHandler {
  override def evolve(dbName: String,
                      tableName: String,
                      metastoreSchema: StructType,
                      targetSchema: StructType,
                      client: IMetaStoreClient): Unit = {
    val compatible = targetSchema.fields.forall { inputField =>
      metastoreSchema.field(inputField.name) match {
        case Some(metastoreField) => metastoreField.dataType == inputField.dataType
        case _ => false
      }
    }
    assert(
      compatible,
      s"Input schema $targetSchema is not compatible with the metastore schema $metastoreSchema. If you wish eel-sdk to automatically evolve the target schema (where possible) then set metastoreSchemaHandler=EvolutionMetastoreSchemaHandler on the HiveSink. Other handlers are also availble, see docs or source"
    )
  }
}

/**
  * An implementation of MetastoreSchemaHandler that requires the input
  * schema to be equal to the metastore schema. Equality is defined
  * as having the same field names with the same types (order is irrelevant).
  *
  * Any missing fields or additional fields not present will cause an
  * exception to be raised.
  *
  * If the schemas are not equal then an exception is raised.
  */
object StrictMetastoreSchemaHandler extends MetastoreSchemaHandler {
  override def evolve(dbName: String,
                      tableName: String,
                      metastoreSchema: StructType,
                      targetSchema: StructType,
                      client: IMetaStoreClient): Unit = {
    assert(
      metastoreSchema.fields.map(field => field.name -> field.dataType) == targetSchema.fields.map(field => field.name -> field.dataType),
      s"Input schema $targetSchema is not equal to the metastore schema $metastoreSchema. If you wish eel-sdk to automatically evolve the target schema (where possible) then set metastoreSchemaHandler=EvolutionMetastoreSchemaHandler on the HiveSink. Other handlers are also availble, see docs or source."
    )
  }
}

/**
  * An implementation of MetastoreSchemaHandler that will evolve the metastore
  * schema were possible to match the incoming data.
  *
  * It will do this by adding missing fields to the end of the current schema.
  * The new fields cannot be added as partition fields as the table will already have been created.
  */
object EvolutionMetastoreSchemaHandler extends MetastoreSchemaHandler with Logging {

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
