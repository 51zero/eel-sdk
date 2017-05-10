# Eel

[![Build Status](https://travis-ci.org/51zero/eel-sdk.svg?branch=master)](https://travis-ci.org/51zero/eel-sdk)
[<img src="https://img.shields.io/maven-central/v/io.eels/eel-core_2.11.svg?label=latest%20release%20for%202.11"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22eel-core_2.11%22)
[<img src="https://img.shields.io/maven-central/v/io.eels/eel-core_2.12.svg?label=latest%20release%20for%202.12"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22eel-core_2.12%22)

Eel is a toolkit for manipulating data in the hadoop ecosystem. By hadoop ecosystem we mean file formats common to the big-data world, such as parquet, orc, csv in locations such as HDFS or Hive tables. In contrast to distributed batch or streaming engines such as [Spark](http://spark.apache.org/) or [Flink](https://flink.apache.org/), Eel is an SDK intended to be used directly in process. Eel is a lower level API than higher level engines like Spark and is aimed for those use cases when you want something like a file API. 
![eel logo](https://raw.githubusercontent.com/eel-sdk/eel/master/eel-core/src/main/graphics/eel_small.png)

### Example Use Cases

* Importing from one source such as JDBC into another source such as Hive/HDFS
* Coalescing multiple files, such as the output from spark, into a single file
* Querying, streaming or reading into memory (relatively) small datasets directly from your process without reaching out to YARN or similar.
* Moving or altering partitions in hive
* Retrieving statistics on existing tables or datasets
* Reading or generating schemas for existing datasets

## Comparisions

Here are some of our notes comparing eel to other tools that offer functionality similar to eel.

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

## Introduction to the API

The core data structure in Eel is the `Frame`. A frame consists of a `Schema`, and zero or more `Row`s which contain values for each field in the schema. 
A frame is conceptually similar to a table in a relational database, or a dataframe in Spark, or a dataset in Flink. 

Frames can be read from a `Source` such as hive tables, jdbc databases, or even programatically from Scala or Java collections.
Frames can be written out to a `Sink` such as a hive table or parquet file.

The current set of sources and sinks include: *Apache Avro*, *Apache Parquet*, *Apache Orc*, *CSV*, *Kafka* (sink only), *HDFS*, *Kudu*, *JDBC*, *Hive*, *Json Files*.

Once you have a reference to a frame, the frame can be manipulated in a similar way to regular Scala collections - many of the methods
share the same name, such as `map`, `filter`, `take`, `drop`, etc. All operations on a frame are lazy - they will only be executed
once an _action_ takes place such as `collect`, `count`, or `save`.

For example, you could load data from a CSV file, drop rows that don't match a predicate, and then save the data back out to a Parquet file
all in a couple of lines of code.

```scala
val source = CsvSource(new Path("input.csv"))
val sink = ParquetSink(new Path("output.pq"))
source.toFrame().filter(_.get("location") == "London").save(sink)
```

### Types Supported

|Eel Datatype|JVM Types|
|-----|-------|
|BigInteger|BigInt|
|Binary|Array of Bytes|
|Byte|Byte|
|DateTime|java.sql.Date|
|Decimal(precision,scale)|BigDecimal|
|Double|Double|
|Float|Float|
|Int|Int|
|Long|Long|
|Short|Short|
|String|String|
|TimestampMillis|java.sql.Timestamp|
|Array|Array, Java collection or Scala Seq|
|Map|Java or Scala Map|

# Sources and Sinks Usage Patterns

The  following examples describe going from a **JDBCSource** to a specific **Sink** and therefore we first need to set up some test **JDBC** data using a **H2** in-memory database with the following code snippet:

```scala
  def executeBatchSql(dataSource: DataSource, sqlCmds: Seq[String]): Unit = {
    val connection = dataSource.getConnection()
    connection.clearWarnings()
    sqlCmds.foreach { ddl =>
      val statement = connection.createStatement()
      statement.execute(ddl)
      statement.close()
    }
    connection.close()
  }
  // Setup JDBC data in H2 in memory database
  val dataSource = new BasicDataSource()
  dataSource.setDriverClassName("org.h2.Driver")
  dataSource.setUrl("jdbc:h2:mem:eel_test_data")
  dataSource.setPoolPreparedStatements(false)
  dataSource.setInitialSize(5)
  val sql = Seq(
    "CREATE TABLE IF NOT EXISTS PERSON(NAME VARCHAR(30), AGE INT, SALARY NUMBER(38,5), CREATION_TIME TIMESTAMP)",
    "INSERT INTO PERSON VALUES ('Fred', 50, 50000.99, CURRENT_TIMESTAMP())",
    "INSERT INTO PERSON VALUES ('Gary', 50, 20000.34, CURRENT_TIMESTAMP())",
    "INSERT INTO PERSON VALUES ('Alice', 50, 99999.98, CURRENT_TIMESTAMP())"
  )
  executeBatchSql(dataSource, sql)
```

## JdbcSource To HiveSink with Parquet Dialect

First let's create a Hive table named **person** in the database **eel_test** which is partitioned by *Title*

_Note the following Hive DDL creates the table for *Parquet* format_

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `eel_test.person` (
   `NAME` string,
   `AGE` int,
   `SALARY` decimal(38,5),
   `CREATION_TIME` timestamp)
PARTITIONED BY (`title` string)
ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/client/eel_test/persons';
```
**Example Create Table**
```sql
hive> CREATE EXTERNAL TABLE IF NOT EXISTS `eel_test.person` (
    >    `NAME` string,
    >    `AGE` int,
    >    `SALARY` decimal(38,5),
    >    `CREATION_TIME` timestamp)
    > PARTITIONED BY (`title` string)
    > ROW FORMAT SERDE
    >    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    > STORED AS INPUTFORMAT
    >    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    > OUTPUTFORMAT
    >    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    > LOCATION '/client/eel_test/persons';
OK
Time taken: 1.474 seconds
```

### Using the HiveSink

```scala
    // Write to a HiveSink from a JDBCSource
    val query = "SELECT NAME, AGE, SALARY, CREATION_TIME FROM PERSON"
    implicit val hadoopFileSystem = FileSystem.get(new Configuration())
    implicit val hiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf())
    JdbcSource(() => dataSource.getConnection, query)
      .withFetchSize(10)
      .toFrame
      .withLowerCaseSchema
      // Transformation - add title to row
      .map { row => 
         if (row.get("name").toString == "Alice") row.add("title", "Mrs") else row.add("title", "Mr") 
      }
      .to(HiveSink("eel_test", "person").withIOThreads(1).withInheritPermission(true))
```

1. The JDBCSource takes a connection function and a SQL query - it will execute the SQL and derive the EEL schema from it - also notice the withFetchSize which caches the number of rows per fetch reducing the number RPC calls to the database server.
2. *hadoopFileSystem* is a *Hadoop File System* object scala implicit required by the HiveSink
3. *hiveMetaStoreClient* is a *Hive metastore client* object scala implicit required by the HiveSink 
4. *withLowerCaseSchema* lowercases all the field names over the *JdbcSource* schema - internally Hive lowercases table objects and columns and therefore the source schema should also match
5. The *map* function performs some *transformation* -  it simply adds a new column called **title** which figures out whether the value should be **Mr** or **Mrs** - *title* is defined as a partition column key on the Hive table.
6. *HiveSink* on the *to* method specifies the target Hive *database* and *table* respectively.
7. *withIOThreads* on the *HiveSink* specifies the number of worker threads where each thread writes to its own file - the default is 4.  This is set to 1 because we don't want to end up with too many files given that the source only has 3 rows.
8. *withInheritPermission* on the *HiveSink* means that when the sink creates new files it should inherit the HDFS permissions from the parent folder - typically this is negated by the default **UMASK** policy set in the hadoop site files.

- Note the **HiveSink** takes care of automatically updating the *HiveMetaStore* when new partitions are added.

### Results shown in Hive
```sql
hive> select * from eel_test.person;
OK
Fred    50      50000.99000     2017-01-24 14:40:50.664 Mr
Gary    50      20000.34000     2017-01-24 14:40:50.664 Mr
Alice   50      99999.98000     2017-01-24 14:40:50.664 Mrs
Time taken: 2.59 seconds, Fetched: 3 row(s)
hive>
```
### Partition layout on HDFS

There should be 2 files created by the *HiveSink* one in the partiton for title called **Mr** and one in **Mrs**.

Here are the partitions using the **hadoop fs -ls** shell command:

```shell
$ hadoop fs -ls /client/eel_test/persons
Found 2 items
drwxrwxrwx   - eeluser supergroup          0 2017-01-24 14:40 /client/eel_test/persons/title=Mr
drwxrwxrwx   - eeluser supergroup          0 2017-01-24 14:40 /client/eel_test/persons/title=Mrs
```

Now let's see if a file was created for the **Mr** partition:
```shell
$ hadoop fs -ls /client/eel_test/persons/title=Mr
Found 1 items
-rw-r--r--   3 eeluser supergroup        752 2017-01-24 14:40 /client/eel_test/persons/title=Mr/eel_2985827854647169_0
```

Now let's see if a file was created for the **Mrs** partition:
```shell
$ hadoop fs -ls /client/eel_test/persons/title=Mrs
Found 1 items
-rw-r--r--   3 eeluser supergroup        723 2017-01-24 14:40 /client/eel_test/persons/title=Mrs/eel_2985828912259519_0
```
### HiveSource Optmizations

The 1.2 release for the **HiveSource** using **Parquet** and **Orc** storage **formats** exploits the following optimizations supported by these formats:

1. **column pruning** or **schema projection** which means providing a read schema - the reader is interested only in certain fields but not all fields written by the writer. The *Parquet* and *Orc* columnar formats does this efficiently without reading the entire row, i.e. only reading the bytes required for those fields. 
2. **predicate push-down** means that filter expressions can applied to the read without reading the entire row - only reading the bytes required for the filter expressions  
3. In addition partition pruning is supported - if a table is organised by partitions then full table scans can be avoided by providing the partition key values 

#### Reading back the data via HiveSource and printing to the console

```scala
    implicit val hadoopFileSystem = FileSystem.get(new Configuration())
    implicit val hiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf())
    HiveSource("eel_test", "person")
      .toFrame()
      .collect()
      .foreach(row => println(row))
```
1. *hadoopFileSystem* is a *Hadoop File System* object scala implicit required by the HiveSource
2. *hiveMetaStoreClient* is a *Hive metastore client* object scala implicit required by the HiveSource
3. *HiveSource* specifies arguments for the Hive *database* and *table* respectively.
4. To get the collection of rows you need to perform the action **collect** on the source's underlying **frame**:  *toFrame().collect()*, then iterate over each row and print it out using *foreach(row => println(row))*

Here are the results of the read:
```
[name = Fred,age = 50,salary = 50000.99000,creation_time = 2017-01-24 13:40:50.664,title = Mr]
[name = Gary,age = 50,salary = 20000.34000,creation_time = 2017-01-24 13:40:50.664,title = Mr]
[name = Alice,age = 50,salary = 99999.98000,creation_time = 2017-01-24 13:40:50.664,title = Mrs]
```

### Using a predicate with the HiveSource

You can query data via the **HiveSource** using simple **and**/**or** predicates with relational operators such as **equals**, **gt**, **ge**, **lt**, **le**, etc...

```scala
    implicit val hadoopFileSystem = FileSystem.get(new Configuration())
    implicit val hiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf())
    HiveSource("eel_test", "person")
      .withPredicate(Predicate.or(Predicate.equals("name", "Alice"), Predicate.equals("name", "Gary")))
      .toFrame()
      .collect()
      .foreach(row => println(row))
```
The above **HiveSource** predicate is equivalent to the SQL:
```sql 
select * from eel_test.person 
where name = 'Alice' or name = 'Gary'
```
The result is as follows:
```
[name = Gary,age = 50,salary = 20000.34000,creation_time = 2017-01-24 13:40:50.664,title = Mr]
[name = Alice,age = 50,salary = 99999.98000,creation_time = 2017-01-24 13:40:50.664,title = Mrs]
```
#### Using a partition key and predicate with the HiveSource

Specifying a partition key on the **HiveSource** using the method **withPartitionConstraint** restricts the *predicate* being performed on a specific *partition*.  This significantly speeds up the query, i.e. avoids an expensive table scan.

If you have simple filtering requirements on relatively small datasets then this approach may be considerably faster than using *Hive*, *Spark*, *Impala* query engines.  Here's an example:

```scala
    implicit val hadoopFileSystem = FileSystem.get(new Configuration())
    implicit val hiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf())
    HiveSource("eel_test", "person")
      .withPredicate(Predicate.or(Predicate.equals("name", "Alice"), Predicate.equals("name", "Gary")))
      .withPartitionConstraint(PartitionConstraint.equals("title", "Mr"))
      .toFrame()
      .collect()
      .foreach(row => println(row))
```
The **withPartitionConstraint** method homes in on the **title** partition whose value is **Mr** and peforms filtering on it using the **withPredicate**.   

The equivalent SQL would be:
```sql 
select * from eel_test.person 
where title = 'Mr'
and (name = 'Alice' or name = 'Gary')
```

The result is as follows:
```
[name = Gary,age = 50,salary = 20000.34000,creation_time = 2017-01-24 13:40:50.664,title = Mr]
```

## JdbcSource To ParquetSink

```scala
  val query = "SELECT NAME, AGE, SALARY, CREATION_TIME FROM PERSON"
  val parquetFilePath = new Path("hdfs://nameservice1/client/eel/person.parquet")
  implicit val hadoopFileSystem = FileSystem.get(new Configuration()) // This is required
  JdbcSource(() => dataSource.getConnection, query).withFetchSize(10)
    .toFrame.to(ParquetSink(parquetFilePath))
```
1. The **JDBCSource** takes a connection function and a SQL query - it will execute the SQL and derive the EEL schema from it - also notice the **withFetchSize** which caches the number of rows per fetch reducing the number RPC calls to the database server.
2. **parquetFilePath** is the **ParquetSink** file path pointing to a **HDFS** path - alternatively this could be a local file path if you qualify it with the *file:* scheme 
3. **hadoopFileSystem** is a scala implicit required by the **ParquetSink**
4. If you have the **parquet-tools** installed on your system you can look at its native schema like so:
```shell
$ parquet-tools schema person.parquet
message row {
  optional binary NAME (UTF8);
  optional int32 AGE;
  optional fixed_len_byte_array(16) SALARY (DECIMAL(38,5));
  optional int96 CREATION_TIME;
}
```
- For **Decimal** Parquet encodes it as a *fixed byte array* and for *Timestamp* it's an *int96*
5. Reading back the data via **ParquetSource** and printing to the console:
```scala
   val parquetFilePath = new Path("hdfs://nameservice1/client/eel/person.parquet")
    implicit val hadoopConfiguration = new Configuration()
    implicit val hadoopFileSystem = FileSystem.get(hadoopConfiguration) // This is required
    ParquetSource(parquetFilePath)
      .toFrame()
      .collect()
      .foreach(row => println(row))
```

1. **parquetFilePath** is the **ParquetSource** file path pointing to a **HDFS** path - alternatively this could be a local file path if you qualify it with the *file:* scheme 
2. **hadoopConfiguration** and **hadoopFileSystem** are scala implicits required by the **ParquetSource**
3. To get the collection of rows you need to perform the action **collect** on the source's underlying **frame**:  *toFrame().collect()*, then iterate over each row and print it out using *foreach(row => println(row))*
4. Here are the results of the read:
```
[NAME = Fred,AGE = 50,SALARY = 50000.99000,CREATION_TIME = 2017-01-23 14:53:51.862]
[NAME = Gary,AGE = 50,SALARY = 20000.34000,CREATION_TIME = 2017-01-23 14:53:51.876]
[NAME = Alice,AGE = 50,SALARY = 99999.98000,CREATION_TIME = 2017-01-23 14:53:51.876]
```

### predicate push-down

You can query data via the **ParquetSource** using simple and/or predicates with relational operators such as **equals**, **gt**, **ge**, **lt**, **le**, etc...

*predicate push-down* means that filter expressions can applied to the read without reading the entire row (features of  **Parquet** and **Orc** columnar formats), i.e. it only reads the bytes required for the filter expressions, e.g.:
```scala
    val parquetFilePath = new Path("hdfs://nameservice1/client/eel/person.parquet")
    implicit val hadoopConfiguration = new Configuration()
    implicit val hadoopFileSystem = FileSystem.get(hadoopConfiguration) // This is required
    ParquetSource(parquetFilePath)
      .withPredicate(Predicate.or(Predicate.equals("NAME", "Alice"), Predicate.equals("NAME", "Gary")))
      .toFrame()
      .collect()
      .foreach(row => println(row))
```
The above **ParquetSource** predicate (**withPredicate**) is equivalent to the SQL predicate:
```sql 
where name = 'Alice' or name = 'Gary'
```
The result is as follows:
```
[NAME = Gary,AGE = 50,SALARY = 20000.34000,CREATION_TIME = 2017-01-23 14:53:51.876]
[NAME = Alice,AGE = 50,SALARY = 99999.98000,CREATION_TIME = 2017-01-23 14:53:51.876]
```
### schema projection 

*column pruning* or *schema projection* which means providing a read schema - the reader is interested only in certain fields but not all fields written by the writer. The *Parquet* and *Orc* columnar formats does this efficiently without reading the entire row, i.e. only reading the bytes required for those fields, e.g.:
```scala
    val parquetFilePath = new Path("hdfs://nameservice1/client/eel/person.parquet")
    implicit val hadoopConfiguration = new Configuration()
    implicit val hadoopFileSystem = FileSystem.get(hadoopConfiguration) // This is required
    ParquetSource(parquetFilePath)
      .withProjection("NAME", "SALARY")
      .withPredicate(Predicate.or(Predicate.equals("NAME", "Alice"), Predicate.equals("NAME", "Gary")))
      .toFrame()
      .collect()
      .foreach(row => println(row))
```
The above **ParquetSource** projection (**withProjection**) is equivalent to the SQL select:
```sql 
select NAME, SALARY
```
The result is as follows:
```
[NAME = Gary,SALARY = 20000.34000]
[NAME = Alice,SALARY = 99999.98000]
```

## JdbcSource To OrcSink

1. The OrcSink is almost identical to the way the parquet sink works (see above)
```scala
    // Write to a OrcSink from a JDBCSource
    val query = "SELECT NAME, AGE, SALARY, CREATION_TIME FROM PERSON"
    val orcFilePath = new Path("hdfs://nameservice1/client/eel/person.orc")
    implicit val hadoopConfiguration = new Configuration()
    JdbcSource(() => dataSource.getConnection, query).withFetchSize(10)
      .toFrame
      .to(OrcSink(orcFilePath))
```
2. Reading back the data via **OrcSource** and printing to the console:
```scala
    val orcFilePath = new Path("hdfs://nameservice1/client/eel/person.orc")
    implicit val hadoopConfiguration = new Configuration()
    OrcSource(orcFilePath)
      .toFrame().collect().foreach(row => println(row))
```
## JdbcSource To KudoSink

**TBD** 

## JdbcSource To AvroSink

```scala
    // Write to a AvroSink from a JDBCSource
    val query = "SELECT NAME, AGE, SALARY, CREATION_TIME FROM PERSON"
    val avroFilePath = Paths.get(s"${sys.props("user.home")}/person.avro")
    JdbcSource(() => dataSource.getConnection, query)
      .withFetchSize(10)
      .toFrame
      .replaceFieldType(DecimalType.Wildcard, DoubleType)
      .replaceFieldType(TimestampMillisType, StringType)
      .to(AvroSink(avroFilePath))
```
1. The **JDBCSource** takes a connection function and a SQL query - it will execute the SQL and derive the EEL schema from it - also notice the **withFetchSize** which caches the number of rows per fetch reducing the number RPC calls to the database server.
2. **avroFilePath** is the **AvroSource** file path pointing to a path on the local file system 
3. The 2 **replaceFieldType** method calls map **DecimalType** to **DoubleType** and **TimestampMillisType** to **StringType** as **Decimals** and **Timestamps** are not supported in *Avro Schema*
4. If you have the **avro-tools** installed on your system you can look at its native schema like so - alternatively use the **AvroSource** to read it back in - see below.
```shell
$ avro-tools getschema person.avro
{
  "type" : "record",
  "name" : "row",
  "namespace" : "namespace",
  "fields" : [ {
    "name" : "NAME",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "AGE",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "SALARY",
    "type" : [ "null", "double" ],
    "default" : null
  }, {
    "name" : "CREATION_TIME",
    "type" : [ "null", "string" ],
    "default" : null
  } ]
}
```
5. Reading back the data via **AvroSource** and printing to the console:
```scala
    val avroFilePath = Paths.get(s"${sys.props("user.home")}/person.avro")
    AvroSource(avroFilePath)
      .toFrame()
      .collect()
      .foreach(row => println(row))
```

1. **avroFilePath** is the **AvroSource** file path pointing to a path on the local file system 
2. To get the collection of rows you need to perform the action **collect** on the source's underlying **frame**:  *toFrame().collect()*, then iterate over each row and print it out using *foreach(row => println(row))*
4. Here are the results of the read:
```
[NAME = Fred,AGE = 50,SALARY = 50000.99,CREATION_TIME = 2017-01-24 16:13:07.524]
[NAME = Gary,AGE = 50,SALARY = 20000.34,CREATION_TIME = 2017-01-24 16:13:07.532]
[NAME = Alice,AGE = 50,SALARY = 99999.98,CREATION_TIME = 2017-01-24 16:13:07.532]
```

## JdbcSource To CsvSink

1. The CsvSink is almost identical to the way the parquet sink works (see above)
```scala
    // Write to a CsvSink from a JDBCSource
    val query = "SELECT NAME, AGE, SALARY, CREATION_TIME FROM PERSON"
    val csvFilePath = new Path("hdfs://nameservice1/client/eel/person.csv")
    implicit val hadoopConfiguration = new Configuration()
    implicit val hadoopFileSystem = FileSystem.get(new Configuration()) // This is required
    JdbcSource(() => dataSource.getConnection, query).withFetchSize(10)
      .toFrame
      .to(CsvSink(csvFilePath))
```
2. Reading back the data via **CsvSource** and printing to the console:
```scala
    val csvFilePath = new Path("hdfs://nameservice1/client/eel/person.csv")
    implicit val hadoopConfiguration = new Configuration()
    implicit val hadoopFileSystem = FileSystem.get(hadoopConfiguration) // This is required
    CsvSource(csvFilePath).toFrame().schema.fields.foreach(f => println(f))
    CsvSource(csvFilePath)
      .toFrame()
      .collect()
      .foreach(row => println(row))
```
Note by default the **CsvSource** converts all types to a string - the following code prints out the fields in the schema:
```scala
    CsvSource(csvFilePath).toFrame().schema.fields.foreach(f => println(f))
```
You can enforce the types on the **CSVSource** by supplying *SchemaInferrer*:
```scala
    val csvFilePath = new Path("hdfs://nameservice1/client/eel/person.csv")
    implicit val hadoopConfiguration = new Configuration()
    implicit val hadoopFileSystem = FileSystem.get(hadoopConfiguration) // This is required
    val schemaInferrer = SchemaInferrer(StringType,
      DataTypeRule("AGE", IntType.Signed),
      DataTypeRule("SALARY", DecimalType.Wildcard),
      DataTypeRule(".*\\_TIME", TimeMillisType))
    CsvSource(csvFilePath).withSchemaInferrer(schemaInferrer)
      .toFrame()
      .collect()
      .foreach(row => println(row))
```
The above **schemaInferrer** object sets up some rules for mapping field name **AGE** to an **int**, **Salary** to a **Decimal** and a field name ending in **TIME** using **REGEX** to a **Timestamp**. 

Note the first parameter on **SchemaInferrer** is *StringType* which means that this is the default type for all fields.

## Working with Nested Type in Sources and Sinks

Storage formats *Parquet* and *Orc* support nested types such as *struct*, *map* and *list*.

### Structs in Parquet
The following example describes how to write rows containing a single struct column named *PERSON_DETAILS*:

```sql
struct PERSON_DETAILS {
    NAME String,
    AGE Int,
    SALARY DECIMAL(38,5),
    CREATION_TIME TIMESTAMP
}
```
#### Step 1:  Set up the hdfs path and scala implicit objects
```scala
    val parquetFilePath = new Path("hdfs://nameservice1/client/eel_struct/person.parquet")
    implicit val hadoopConfiguration = new Configuration()
    implicit val hadoopFileSystem = FileSystem.get(hadoopConfiguration) 
```
#### Step 2:  Create the schema containing a single column named *PERSON_DETAILS* which is a *struct* type:
```scala
    val personDetailsStruct = Field.createStructField("PERSON_DETAILS",
      Seq(
        Field("NAME", StringType),
        Field("AGE", IntType.Signed),
        Field("SALARY", DecimalType(Precision(38), Scale(5))),
        Field("CREATION_TIME", TimestampMillisType)
      )
    )
    val schema = StructType(personDetailsStruct)
```
- A *struct* is encoded as a list of *Fields* with their corresponding *type* definitions.

#### Step 3:  Create 3 rows of *structs*
```scala
    val rows = Vector(
      Vector(Vector("Fred", 50, BigDecimal("50000.99000"), new Timestamp(System.currentTimeMillis()))),
      Vector(Vector("Gary", 50, BigDecimal("20000.34000"), new Timestamp(System.currentTimeMillis()))),
      Vector(Vector("Alice", 50, BigDecimal("99999.98000"), new Timestamp(System.currentTimeMillis())))
    )
```
- The first *Vector*, e.g.  **val rows = Vector(...)** is a list of rows - 3 in this case.
- Each inner *Vector*, e.g. **Vector(...)** is a single row of column values
- The column values in this case is another **Vector** representing the the **struct**, e.g. **Vector("Alice", 50, BigDecimal("99999.98000"), new Timestamp(System.currentTimeMillis()))**

#### Step 4:  Write the rows using the ParquetSink
```scala
    Frame.fromValues(schema, rows)
      .to(ParquetSink(parquetFilePath))
```

If you have the **parquet-tools** installed on your system you can look at its native schema like so:
```shell
$ parquet-tools schema person.parquet
message row {
  optional group PERSON_DETAILS {
    optional binary NAME (UTF8);
    optional int32 AGE;
    optional fixed_len_byte_array(16) SALARY (DECIMAL(38,5));
    optional int96 CREATION_TIME;
  }
}
```
- Notice that parquet encodes the *struct* as *group* of columns.
#### Step 5:  Read back the rows using the ParquetSource
```scala
    ParquetSource(parquetFilePath)
      .toFrame()
      .collect()
      .foreach(row => println(row))
```
#### The results of Step 5
```
[PERSON_DETAILS = WrappedArray(Fred, 50, 50000.99000, 2017-01-25 15:56:06.212)]
[PERSON_DETAILS = WrappedArray(Gary, 50, 20000.34000, 2017-01-25 15:56:06.212)]
[PERSON_DETAILS = WrappedArray(Alice, 50, 99999.98000, 2017-01-25 15:56:06.212)]
```
#### Applying a predicate (filter) on the read - give me person details for names Alice and Gary
```scala
    ParquetSource(parquetFilePath)
      .withPredicate(Predicate.or(Predicate.equals("PERSON_DETAILS.NAME", "Alice"), Predicate.equals("PERSON_DETAILS.NAME", "Gary")))
      .toFrame()
      .collect()
      .foreach(row => println(row))
```
The above is equivalent to the following in SQL:
```sql
select PERSON_DETAILS
where PERSON_DETAILS.NAME = 'Alice' or PERSON_DETAILS.NAME = 'Gary'
``` 
#### The results with the predicate filter
```
[PERSON_DETAILS = WrappedArray(Gary, 50, 20000.34000, 2017-01-25 16:03:37.678)]
[PERSON_DETAILS = WrappedArray(Alice, 50, 99999.98000, 2017-01-25 16:03:37.678)]
```

### Looking at the **Parquet** file through **Hive**

On the *Parquet* file just written we can create a **Hive External** table pointing at the *HDFS* location of the file.
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `eel_test.struct_person`(
   PERSON_DETAILS STRUCT<NAME:String, AGE:Int, SALARY:decimal(38,5), CREATION_TIME:TIMESTAMP>
)
ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/client/eel_struct';
```
- The location **/client/eel_struct** is the root directory of where all the files live - in this case its the root of folder of the *Parquet* write in *step 4*.

#### Here's a Hive session show the select:
```sql
hive> select * from eel_test.struct_person;
OK
{"NAME":"Fred","AGE":50,"SALARY":50000.99,"CREATION_TIME":"2017-01-25 17:03:37.678"}
{"NAME":"Gary","AGE":50,"SALARY":20000.34,"CREATION_TIME":"2017-01-25 17:03:37.678"}
{"NAME":"Alice","AGE":50,"SALARY":99999.98,"CREATION_TIME":"2017-01-25 17:03:37.678"}
Time taken: 1.092 seconds, Fetched: 3 row(s)
hive>
```
#### Here's another Hive query asking for Alice and Gary's age:
```sql
hive> select person_details.name, person_details.age
    > from eel_test.struct_person
    > where person_details.name in ('Alice', 'Gary' );
OK
Gary    50
Alice   50
Time taken: 0.067 seconds, Fetched: 2 row(s)
hive>
```
-  *HiveQL* has some nice features for cracking nested types - the query returns scalar values for *name* and *age* in the *person_details* structure.
-  The same query is supported in *Spark* via *HiveContext* or *SparkSession* in version *>= 2.x*

### Arrays in Parquet

EEL supports *Parquet* **ARRAYS** of any *primitive* type including *structs*.  The following example extends the previous example by adding another column called **PHONE_NUMBERS** defined as an **ARRAY** of **Strings**.   

#### Writing with an ARRAY of strings - PHONE_NUMBERS
```scala
    val parquetFilePath = new Path("hdfs://nameservice1/client/eel_array/person.parquet")
    implicit val hadoopConfiguration = new Configuration()
    implicit val hadoopFileSystem = FileSystem.get(hadoopConfiguration) 
   // Create the schema with a STRUCT and an ARRAY
    val personDetailsStruct = Field.createStructField("PERSON_DETAILS",
      Seq(
        Field("NAME", StringType),
        Field("AGE", IntType.Signed),
        Field("SALARY", DecimalType(Precision(38), Scale(5))),
        Field("CREATION_TIME", TimestampMillisType)
      )
    )
    val schema = StructType(personDetailsStruct, Field("PHONE_NUMBERS", ArrayType.Strings))

    // Create 3 rows
    val rows = Vector(
      Vector(Vector("Fred", 50, BigDecimal("50000.99000"), new Timestamp(System.currentTimeMillis())), Vector("322", "987")),
      Vector(Vector("Gary", 50, BigDecimal("20000.34000"), new Timestamp(System.currentTimeMillis())), Vector("145", "082")),
      Vector(Vector("Alice", 50, BigDecimal("99999.98000"), new Timestamp(System.currentTimeMillis())), Vector("534", "129"))
    )
   // Write the rows
    Frame.fromValues(schema, rows)
      .to(ParquetSink(parquetFilePath))
```
If you have the **parquet-tools** installed on your system you can look at its native schema like so:
```shell
$ parquet-tools schema person.parquet
message row {
  optional group PERSON_DETAILS {
    optional binary NAME (UTF8);
    optional int32 AGE;
    optional fixed_len_byte_array(16) SALARY (DECIMAL(38,5));
    optional int96 CREATION_TIME;
  }
  repeated binary PHONE_NUMBERS (UTF8);
}
```
- Notice **PHONE_NUMBERS** is represented as a repeated UTF8 (String) in Parquet, i.e. an unbounded array.
#### Read back the rows via ParquetSource
```scala
    ParquetSource(parquetFilePath)
      .toFrame()
      .collect()
      .foreach(row => println(row))
```
- The results
```
[PERSON_DETAILS = WrappedArray(Fred, 50, 50000.99000, 2017-01-25 20:33:48.302),PHONE_NUMBERS = Vector(322, 987)]
[PERSON_DETAILS = WrappedArray(Gary, 50, 20000.34000, 2017-01-25 20:33:48.302),PHONE_NUMBERS = Vector(145, 082)]
[PERSON_DETAILS = WrappedArray(Alice, 50, 99999.98000, 2017-01-25 20:33:48.302),PHONE_NUMBERS = Vector(534, 129)]
```
### Looking at the **Parquet** file through **Hive**

On the *Parquet* file just written we can create a **Hive External** table pointing at the *HDFS* location of the file.
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `eel_test.struct_person_phone`(
   PERSON_DETAILS STRUCT<NAME:String, AGE:Int, SALARY:decimal(38,5), CREATION_TIME:TIMESTAMP>,
   PHONE_NUMBERS Array<String>
)
ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/client/eel_array';
```
- The location **/client/eel_array** is the root directory of where all the files live - in this case its the root of folder of the *Parquet* write 

#### Here's a Hive session show the select:
```sql
hive> select * from eel_test.struct_person_phone;
OK
{"NAME":"Fred","AGE":50,"SALARY":50000.99,"CREATION_TIME":"2017-01-26 10:50:57.192"}    ["322","987"]
{"NAME":"Gary","AGE":50,"SALARY":20000.34,"CREATION_TIME":"2017-01-26 10:50:57.192"}    ["145","082"]
{"NAME":"Alice","AGE":50,"SALARY":99999.98,"CREATION_TIME":"2017-01-26 10:50:57.192"}   ["534","129"]
Time taken: 1.248 seconds, Fetched: 3 row(s)
hive>
```
#### Here's another Hive query asking for Alice and Gary's age and phone numbers:
```sql
hive> select person_details.name, person_details.age, phone_numbers
    > from eel_test.struct_person_phone
    > where person_details.name in ('Alice', 'Gary' );
OK
Gary    50      ["145","082"]
Alice   50      ["534","129"]
Time taken: 0.181 seconds, Fetched: 2 row(s)
hive>
```
-  *HiveQL* has some nice features for cracking nested types - the query returns scalar values for *name* and *age* in the *person_details* structure and phone numbers from the phone_numbers array.
-  The same query is supported in *Spark* via *HiveContext* or *SparkSession* in version *>= 2.x*

#### What if I want to look at the first phone number:
```sql
hive> select person_details.name, person_details.age, phone_numbers[0]
    > from eel_test.struct_person_phone;
OK
Fred    50      322
Gary    50      145
Alice   50      534
Time taken: 0.08 seconds, Fetched: 3 row(s)
hive>
```
- To retrieve a specific array element, **HiveQL** requires the column index which is zero based, e.g. **phone_numbers[0]**

#### Query to show *name*, *age* and *phone_number* with repeated rows for each phone number from the phone_numbers array
```sql
hive> select person_details.name, person_details.age, phone_number
    > from eel_test.struct_person_phone
    > lateral view explode(phone_numbers) pns as phone_number;
OK
Fred    50      322
Fred    50      987
Gary    50      145
Gary    50      082
Alice   50      534
Alice   50      129
Time taken: 0.062 seconds, Fetched: 6 row(s)
hive>
```
- The above **lateral view** statement is used in conjunction with the **explode UDTF(user-defined-table-function)** to generate a row per array element 


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
