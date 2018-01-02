package io.eels.component.hbase

import java.nio

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.datastream.Publisher
import io.eels.schema.{Field, StringType, StructType}
import org.apache.commons.net.ntp.TimeStamp
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Consistency, IsolationLevel}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.security.UserGroupInformation

object HbaseSource {
  private val unverifiedField = Field("eel_dummy", StringType)
  private val unverifiedSchema = StructType(Vector(unverifiedField))

  def withHiveMappings(database: String, table: String): HbaseSource = {
    val hbaseHiveInfo = HbaseHiveOps.hbaseInfoFromHive(database, table)
    HbaseSource(hbaseHiveInfo.namespace, hbaseHiveInfo.table, serializer = hbaseHiveInfo.hbaseSerializer, schema = hbaseHiveInfo.schema)
  }
}

case class HbaseSource(namespace: String,
                       table: String,
                       principal: Option[String] = None,
                       keytabPath: Option[nio.file.Path] = None,
                       caching: Option[Int] = None,
                       cacheBlocks: Option[Boolean] = None,
                       batch: Option[Int] = None,
                       startKey: Option[AnyRef] = None,
                       stopKey: Option[AnyRef] = None,
                       stopKeyInclusive: Boolean = false,
                       consistency: Option[Consistency] = None,
                       isolationLevel: Option[IsolationLevel] = None,
                       timeRange: Option[(Long, Long)] = None,
                       timeStamp: Option[Long] = None,
                       rowPrefixFilter: Option[Array[Byte]] = None,
                       maxVersions: Option[Int] = None,
                       maxResultsPerColumnFamily: Option[Int] = None,
                       rowOffsetPerColumnFamily: Option[Int] = None,
                       maxResultSize: Option[Long] = None,
                       reverseScan: Option[Boolean] = None,
                       allowPartialResults: Option[Boolean] = None,
                       loadColumnFamiliesOnDemand: Option[Boolean] = None,
                       returnDeletedRows: Option[Boolean] = None,
                       identifier: Option[String] = None,
                       filterList: Option[FilterList] = None,
                       maxRows: Long = Long.MaxValue,
                       bufferSize: Int = 100,
                       implicit val schema: StructType = HbaseSource.unverifiedSchema,
                       implicit val serializer: HbaseSerializer = HbaseSerializer.standardSerializer,
                       implicit val connection: Connection = ConnectionFactory.createConnection(HBaseConfiguration.create())
                      ) extends Source with Logging with Using with AutoCloseable {

  override def parts(): Seq[Publisher[Seq[Row]]] = Seq(new HbasePublisher(connection, schema, namespace, table, bufferSize, maxRows, HbaseScanner(this), serializer))

  override def close(): Unit = connection.close()

  def statistics(): HbaseStatistics = HbaseStatistics(namespace, table)

  def withFieldKey(name: String): HbaseSource = withFieldKey(schema.fields.find(_.name == name).getOrElse(sys.error(s"Field '$name' not found in schema")))

  def withFieldKey(field: Field): HbaseSource = copy(
    schema = schema.addField(Field(field.name, field.dataType, field.nullable, field.partition, field.comment, key = true, field.defaultValue, field.metadata))
  )

  def withSchema(schema: StructType): HbaseSource = copy(schema = schema)

  def withFieldsFromHiveSchema(hiveDatabase: String, hiveTable: String): HbaseSource = {
    copy(schema = StructType(HbaseHiveOps.fromHiveHbaseSchema(hiveDatabase, hiveTable)))
  }

  def withKeyValue(startKey: AnyRef): HbaseSource = withKeyValueRange(startKey, stopKey)

  def withKeyValueRange(startKey: AnyRef, stopKey: AnyRef, stopKeyInclusive: Boolean = false): HbaseSource = {
    copy(startKey = Option(startKey), stopKey = Option(stopKey), stopKeyInclusive = stopKeyInclusive)
  }

  def withField(cf: String, field: Field): HbaseSource = {
    copy(schema = schema.addField(
      Field(field.name, field.dataType, field.nullable, field.partition, field.comment, key = field.key, field.defaultValue, field.metadata, columnFamily = Option(cf)))
    )
  }

  def withCacheBlocks(cacheBlocks: Boolean): HbaseSource = copy(cacheBlocks = Option(cacheBlocks))

  def withCaching(caching: Int): HbaseSource = copy(caching = Option(caching))

  def withConsistency(consistency: Consistency): HbaseSource = copy(consistency = Option(consistency))

  def withIsolationLevel(isolationLevel: IsolationLevel): HbaseSource = copy(isolationLevel = Option(isolationLevel))

  def withTimeRange(minStamp: Long, maxStamp: Long): HbaseSource = copy(timeRange = Option((minStamp, maxStamp)))

  def withTimeRange(minStamp: TimeStamp, maxStamp: TimeStamp): HbaseSource = copy(timeRange = Option((minStamp.getTime, maxStamp.getTime)))

  def withTimeSTamp(timeStamp: Long): HbaseSource = copy(timeStamp = Option(timeStamp))

  def withRowPrefixFilter(rowPrefixFilter: Array[Byte]): HbaseSource = copy(rowPrefixFilter = Option(rowPrefixFilter))

  def withMaxVersions(maxVersions: Int): HbaseSource = copy(maxVersions = Option(maxVersions))

  def withMaxResultsPerColumnFamily(maxResultsPerColumnFamily: Int): HbaseSource = copy(maxResultsPerColumnFamily = Option(maxResultsPerColumnFamily))

  def withRowOffsetPerColumnFamily(rowOffsetPerColumnFamily: Int): HbaseSource = copy(rowOffsetPerColumnFamily = Option(rowOffsetPerColumnFamily))

  def withMaxResultSize(maxResultSize: Int): HbaseSource = copy(maxResultSize = Option(maxResultSize))

  def withReverseScan(reverseScan: Boolean): HbaseSource = copy(reverseScan = Option(reverseScan))

  def withAllowPartialResults(allowPartialResults: Boolean): HbaseSource = copy(allowPartialResults = Option(allowPartialResults))

  def withLoadColumnFamiliesOnDemand(loadColumnFamiliesOnDemand: Boolean): HbaseSource = copy(loadColumnFamiliesOnDemand = Option(loadColumnFamiliesOnDemand))

  def withReturnDeletedRows(returnDeletedRows: Boolean): HbaseSource = copy(returnDeletedRows = Option(returnDeletedRows))

  def withIdentifier(identifier: String): HbaseSource = copy(identifier = Option(identifier))

  def withPredicate(pred: Predicate): HbaseSource = addFilterList(HbasePredicate(pred))

  // This maybe more efficient (skip row reads) than simply using "HbasePredicate.notEquals" on its own.
  // Usage: HbaseSource.withSkpRowsPredicate(HbasePredicate.notEquals("currency", "EUR"))
  def withSkipRowsPredicate(pred: Predicate): HbaseSource = {
    import scala.collection.JavaConversions._
    val filters = HbasePredicate(pred).getFilters.map {
      case svf: SingleColumnValueFilter => new SkipFilter(new ValueFilter(svf.getOperator, svf.getComparator))
      case rf: RowFilter => new SkipFilter(new ValueFilter(rf.getOperator, rf.getComparator))
      case _@unknownFilter => sys.error(s"'${unknownFilter.getClass.getSimpleName}' filter is not supported for skipping rows")
    }
    addFilterList(new FilterList(filters))
  }

  def withHbaseFilterList(filterList: FilterList): HbaseSource = addFilterList(filterList)

  def withLimitRows(limitRows: Long): HbaseSource = addFilterList(new FilterList(new PageFilter(maxRows))).copy(maxRows = limitRows)

  def withBufferSize(bufferSize: Int): HbaseSource = copy(bufferSize = bufferSize)

  def withProjection(first: String, rest: String*): HbaseSource = withProjection(first +: rest)

  def withProjection(fields: Seq[String]): HbaseSource = {
    require(fields.nonEmpty)
    copy(schema = StructType(schema.fields.filter(f => fields.contains(f.name) || f.key)))
  }

  def withKeytabFile(principal: String, keytabPath: nio.file.Path): HbaseSource = {
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath.toString)
    copy(principal = Option(principal), keytabPath = Option(keytabPath))
  }

  def withConfiguration(configuration: HBaseConfiguration): HbaseSource = copy(connection = ConnectionFactory.createConnection(configuration))

  def withSerializer(serializer: HbaseSerializer): HbaseSource = copy(serializer = serializer)

  private def addFilterList(newFilterList: FilterList): HbaseSource = {
    copy(filterList = Option(if (filterList.nonEmpty) new FilterList(filterList.head, newFilterList) else newFilterList))
  }

}
