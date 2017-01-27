# Eel

[![Build Status](https://travis-ci.org/sksamuel/eel-sdk.svg?branch=master)](https://travis-ci.org/sksamuel/eel-sdk)
[<img src="https://img.shields.io/maven-central/v/io.eels/eel-core_2.11*.svg?label=latest%20release%20for%202.11"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22eel-core_2.11%22)
[<img src="https://img.shields.io/maven-central/v/io.eels/eel-core_2.12*.svg?label=latest%20release%20for%202.12"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22eel-core_2.12%22)

Eel is a toolkit for manipulating data in the hadoop ecosystem. By hadoop ecosystem we mean file formats common to the big-data world, such as parquet, orc, csv in locations such as HDFS or Hive tables. In contrast to distributed batch or streaming engines such as [Spark](http://spark.apache.org/) or [Flink](https://flink.apache.org/), Eel is an SDK intended to be used directly in process. Eel is a lower level API than higher level engines like Spark and is aimed for those use cases when you want something like a file API. 
![eel logo](https://raw.githubusercontent.com/eel-sdk/eel/master/eel-core/src/main/graphics/eel_small.png)

### Example Use Cases

* Importing from one source such as JDBC into another source such as Hive/HDFS
* Coalescing multiple files, such as the output from spark, into a single file
* Querying, streaming or reading into memory (relatively) small datasets directly from your process without reaching out to YARN or similar.
* Moving or altering partitions in hive
* Retrieving statistics on existing tables or datasets
* Reading schemas for existing datasets

## Comparison with Sqoop

*Sqoop* is a popular Hadoop ETL tool and API used for loading foreign data (e.g. JDBC) into Hive/Hadoop 

Sqoop executes N configurable Hadoop mappers jobs which are executed in parallel. Each mapper job makes a separate JDBC connection and adapts their queries to retrieve parts of the data.  

To support the parallelism of mapper jobs you must specify a **split by** column key and Hive partitioning key columns if applicable.

- With this approach you can end up with several small part files (one for each mapper task) in HDFS which is not the most optimal way of storing data in Hadoop.
- To reduce the number of part files you must reduce the number of mappers hence reducing the parallelism 
- At the time of this writing Oracle **Number** and **Timestamp** types aren't properly supported from Oracle to Hive with a Parquet dialect
- **Sqoop** depends on **YARN** to allocate resources for each mapper task
- Both the **Sqoop** CLI and API has a steep learning curve 

## Comparison with Flume

*Flume* supports streaming data from a plethora of out-of-the-box Sources and Sinks.  

Flume supports the notion of a channel which is like a persistent queue and glues together sources and sinks.  

The channel is an attractive feature as it can buffer up transactions/events under heavy load conditions – channel types can be File, Kafka or JDBC.

- The Flume Hive sink is *limited* to streaming events containing delimited text or JSON data directly into a Hive table or partition - it’s possible to write a Custom EEL source and sink and therefore supporting all source/sink types such as Parquet, Orc, Hive, etc...
- Flume requires an additional maintenance of a Flume Agent topology - separate processes.

## Comparison with Kite

The Kite API and CLI are very similar in functionality to EEL but there are some subtle differences:

- Datasets in *Kite* require AVRO schemas
- A dataset is essentially a Hive table - the upcoming *EEL 1.2* release you will be able to create Hive tables from the CLI - at the moment it’s possible generate the Hive DDL with EEL API using *io.eels.component.hive.HiveDDL$#showDDL*.
- For writing directly to **AVRO** or **Parquet** storage formats you must provide an **AVRO** schema – EEL dynamically infers a schema from the underlying source, for example a JDBC Query or CSV headers.
- Support for ingesting from storage formats (other than **AVRO** and **Parquet**) is be achieved by *transforming* each record/row with another module named **Kite Morphlines** - it uses another intermediate record format and is another **API** to learn.
- EEL supports transformations using regular Scala functions by invoking the *map* method on the Source’s underlying *Frame*, e.g. *source.toFrame.map(f: (Row) => Row)* – the *map* function returns a new row object.
- Kite has direct support for *HBase* but EEL doesn’t – will do with the upcoming *EEL 1.2* release
- Kite currently **doesn’t** support Kudo – EEL does.
- Kite stores additional metadata on disk (**HDFS**) to be deemed a valid Kite dataset – if you externally change the Schema outside of Kite, i.e. through *DDL* then it can cause a dataset to be *out-of-synch* and potentially *malfunction* - EEL functions normally in this scenario as there is no additional metadata required.
- Kite handles Hive partitioning by specifying a partition strategies – there are a few *out-of-the-box* strategies derived from the current payload – with **EEL** this works auto-magically by virture of providing the same column on the source row, alternatively you can add a partition key column with  **addField** on the fly on the source’s frame or use **map** transformation function.

## Frames

The core data structure in Eel is the Frame. A frame consists of a schema, and rows which contain values for each field. A frame is conceptually similar to a table in a relational database, or a dataframe in Spark, or a dataset in Flink. Frames are constructed from sources such as hive tables, jdbc databases, delimited files, kafka queues, or even programatically from Scala or Java collections.

### Types Supported

|Eel Datatype|JVM Types|
|-----|-------|
|Short|Short|
|Int|Int|
|Double|Double|
|Float|Float|
|Decimal|BigDecimal|
|String|String|
|Binary|Array of Bytes|
|DateTime|java.sql.Date|
|Timestamp|java.sql.Timestamp|

### Examples

```scala
// copying from avro files to jdbc
AvroSource("test.avro").to(JdbcSink("jdbc:....", "totable", JdbcSinkProps(createTable = true)))
```

```scala
// copying from one jdbc source to another
JdbcSource("jdbc:....", "fromtable").to(JdbcSink("jdbc:....", "totable", JdbcSinkProps(createTable = true)))
```

## Sources

* Apache Avro
* Apache Parquet
* Apache Orc
* CSV
* Hadoop Sequence files
* HDFS 
* Json
* JDBC

## Parquet Source
The parquet source will read from one or more parquet files. To use the source, create an instance of `ParquetSource` specifying a file pattern or `Path` object. The Parquet source implementation is optimized to use native parquet reading directly to an eel row object without creating intermediate formats such as Avro.

Example reading from a single file `ParquetSource(new Path("hdfs:///myfile"))`
Example reading from a wildcard pattern `ParquetSource("hdfs:///user/warehouse/*"))`

#### Predicates

Parquet as a file format supports predicates, which are row level filter operations. Because parquet is a columnar store,
row level filters can be extremely efficient. Whenever you are reading from parquet files - either directly or through hive - 
a row level filter will nearly always be faster than reading the data and filtering afterwards. This is
because parquet is able to skip whole chunks of the file that do not match the predicate.
                                              
To use a predicate, simply add an instance of `Predicate` to the Parquet source class.

```scala
val frame = ParquetSource(path).withPredicate(Predicate.equals("location", "westeros")).toFrame()
```

Multiple predicates can be grouped together using `Predicate.or` and `Predicate.and`.

#### Projections

The parquet source also allows you to specify a projection which is a subset of the columns to return.
Again, since parquet is columnar, if a column is not needed at all then the entire column can be skipped directly 
in the file making parquet extremely fast at this kind of operation.

To use a projection, simply use `withProjection` on the Parquet source with the fields to keep.

```scala
val frame = ParquetSource(path).withProjection("amount", "type").toFrame()
```



Hive Source
---
The [Hive](https://hive.apache.org/) source will read from a hive table. To use this source, create an instance of `HiveSource` specifying the database name, the table name, any partitions to limit the read. The source also requires instances of the Hadoop [Filesystem](https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/fs/FileSystem.html) object, and a [HiveConf](https://hive.apache.org/javadocs/r0.13.1/api/common/org/apache/hadoop/hive/conf/HiveConf.html) object.

Reading all rows from a table is the simplest use case: `HiveSource("mydb", "mytable")`. We can also read rows from a table for a particular partition. For example, to read all rows which have the value '1975' for the partition column 'year': `HiveSource("mydb", "mytable").withPartition("year", "1975")`

The partition clause accepts an operator to perform more complicated querying, such as less than, greater than etc. For example to read all rows which have a *year* less than *1975* we can do: `HiveSource("mydb", "mytable").withPartition("year", "<", "1975")`.


Hive Sink
----
The [Hive](https://hive.apache.org/) sink writes data to Hive tables stored in any of the following formats: ORC (Optimized Row Columnar), Parquet, Avro, or Text delimited.

To configure a Hive Sink, you specify the Hive database, the table to write to, and the format to write in. The sink also requires instances of the Hadoop [Filesystem](https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/fs/FileSystem.html) object, and a [HiveConf](https://hive.apache.org/javadocs/r0.13.1/api/common/org/apache/hadoop/hive/conf/HiveConf.html) object.

**Properties**

|Parameter|Description|
|----------|------------------|
|IO Threads|The number of concurrent writes to the sink|
|Dynamic Partitioning|If set to true then any values on partitioned fields that are new, will automatically be created as partitions in the metastore. If set to false, then a new value will throw an error.

**Example**

Simple example of writing to a Hive database `frame.to(HiveSink("mydb", "mytable"))`

We can specify the number of concurrent writes, by using the ioThreads parameter `frame.to(HiveSink("mydb", "mytable").withIOThreads(4))`
 
Csv Source
----

If the schema you need is in the form of the CSV headers, then we can easily parse those to create the schema. But obviously CSV won't encode any type information. Therefore, we can specify an instance of a `SchemaInferrer` which can be customized with rules to determine the correct schema type for each header. So for example, you might say that "name" is a SchemaType.String, or that anything matching "*_id" is a SchemaType.Long. You can also specify the nullability, scale, precision and unsigned. A quick example:

```scala
val inferrer = SchemaInferrer(SchemaType.String, SchemaRule("qty", SchemaType.Int, false), SchemaRule(".*_id", SchemaType.Int))
CsvSource("myfile").withSchemaInferrer(inferrer)
```

### How to use

Eel is released to maven central, so is very easy to include in your project. Just find the latest version on [maven central](http://search.maven.org/#search|ga|1|io.eels) and copy the includes.
