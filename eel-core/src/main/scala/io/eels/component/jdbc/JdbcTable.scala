package io.eels.component.jdbc

import java.sql.{Connection, DatabaseMetaData, ResultSet}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import com.sksamuel.exts.jdbc.ResultSetIterator
import io.eels.component.jdbc.dialect.{GenericJdbcDialect, JdbcDialect}
import io.eels.schema.{Field, StructType}

case class JdbcTable(tableName: String,
                     dialect: JdbcDialect = new GenericJdbcDialect,
                     catalog: Option[String] = None,
                     dbSchema: Option[String] = None)
                    (implicit conn: Connection) extends Logging with JdbcPrimitives with Using {

  private val databaseMetaData: DatabaseMetaData = conn.getMetaData
  private val tables = ResultSetIterator
    .strings(databaseMetaData.getTables(catalog.orNull, dbSchema.orNull, null, Array("TABLE", "VIEW")))
    .toList
    .map(_ (2))
  private val dbPrefix: String = if (dbSchema.nonEmpty) dbSchema.get + "." else ""

  val candidateTableName: String = tables.find(_.toLowerCase == tableName.toLowerCase).getOrElse(sys.error(s"$tableName not found!"))
  val primaryKeys: Seq[String] = RsIterator(databaseMetaData.getPrimaryKeys(catalog.orNull, dbSchema.orNull, candidateTableName))
    .map(_.getString("COLUMN_NAME")).toSeq

  val schema = StructType(
    JdbcSchemaFns
      .fromJdbcResultset(conn.createStatement().executeQuery(s"SELECT * FROM $dbPrefix$candidateTableName WHERE 1=0"), dialect)
      .fields
      .map { f =>
        Field(name = f.name,
          dataType = f.dataType,
          nullable = f.nullable,
          key = primaryKeys.contains(f.name),
          metadata = f.metadata)
      }
  )

  private case class RsIterator(rs: ResultSet) extends Iterator[ResultSet] {
    def hasNext: Boolean = rs.next()

    def next(): ResultSet = rs
  }

}

