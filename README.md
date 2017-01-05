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
* Reading schemas for existing datasets

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

* CSV
* Json
* JDBC
* Parquet
* Avro
* Hadoop Sequence files
* Hadoop Orc

### Parquet Source
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
