package io.eels.component.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import com.sksamuel.exts.metrics.Timed
import io.eels.component.jdbc.dialect.{GenericJdbcDialect, JdbcDialect}
import io.eels.{Part, Source}
import io.eels.schema.StructType

object JdbcSource {
  def apply(url: String, query: String): JdbcSource = JdbcSource(() => DriverManager.getConnection(url), query)
}

case class JdbcSource(connFn: () => Connection,
                      query: String,
                      bindFn: (PreparedStatement) => Unit = stmt => (),
                      fetchSize: Int = 100,
                      providedSchema: Option[StructType] = None,
                      providedDialect: Option[JdbcDialect] = None,
                      partitionStrategy: JdbcPartitionStrategy = SinglePartitionStrategy)
  extends Source with JdbcPrimitives with Logging with Using with Timed {

  override lazy val schema: StructType = providedSchema.getOrElse(fetchSchema())

  def withBind(bind: (PreparedStatement) => Unit): JdbcSource = copy(bindFn = bind)
  def withFetchSize(fetchSize: Int): JdbcSource = copy(fetchSize = fetchSize)
  def withProvidedSchema(schema: StructType): JdbcSource = copy(providedSchema = Option(schema))
  def withProvidedDialect(dialect: JdbcDialect): JdbcSource = copy(providedDialect = Option(dialect))
  def withPartitionStrategy(strategy: JdbcPartitionStrategy): JdbcSource = copy(partitionStrategy = strategy)

  private def dialect(): JdbcDialect = providedDialect.getOrElse(new GenericJdbcDialect())

  override def parts(): Seq[Part] = partitionStrategy.parts(connFn, query, bindFn, fetchSize, dialect())

  def fetchSchema(): StructType = {
    using(connFn()) { conn =>
      val schemaQuery = s"SELECT * FROM ($query) tmp WHERE 1=0"
      using(conn.prepareStatement(schemaQuery)) { stmt =>

        stmt.setFetchSize(fetchSize)
        bindFn(stmt)

        val rs = timed(s"Executing query $query") {
          stmt.executeQuery()
        }

        val schema = schemaFor(dialect(), rs)
        rs.close()
        schema
      }
    }
  }
}