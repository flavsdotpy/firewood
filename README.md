# Firewood

Firewood is a Java framework created to simplify and speed up the creation of replication, ingestion and transformation jobs using Spark Framework.
You can use it as a library, adding it to the dependencies of your code and calling it programmatically or standalone, calling a bunch of ready-to-use codes.

## Contents of this guide

* Using Firewood as a library
* Using Firewood standalone
* Currently supported input sources
* Currently supported output sources
* Currently supported utility sources
* Property guide
* Suggested reading

## Using Firewood as a library

To use Firewood as a library, you must install it into your maven repository:

```bash
mvn install
```

Add Faio to your dependencies:

```
<dependency>
	<groupId>com.ap3x</groupId>
	<artifactId>firewood</artifactId>
	<version>${firewood-version}</version>
</dependency>
```

In your code, initialize Faio like that:

```
FaioContext  faio = FaioStarter.startFaio(YourConfiguration.class, "faio.properties");
```

With faio context instantiated, you can start getting its Readers, Writers, Transformers, Helpers and even your custom classes that were added to spring context.

_Obs: for better results, use Spring Framework when developing your code, it will fit Faio well_

## Using Faio standalone

_Obs:  currently only supported at AWS with external `.properties` file_

Using Faio standalone is a lot simple.

- Build your own `.properties` file
- Upload it to a s3 bucket that the EMR/Hadoop machine can access
- Build the package with dependencies **(TBD a new Maven step to do this automatically)**
- Upload this package to a s3 bucket that the EMR/Hadoop machine can access
- Execute it at  EMR/Hadoop cluster like this:
`spark-submit <ALL_SPARK_ARGS> --class DataConnector <PATH_TO_JAR>/faio-standalone.jar <BUCKET_OF_PROPERTIES_FILE> <PATH_TO_PROPERTIES_FILE>.properties`

## Currently supported input sources

- **Stream Sources**
	- Apache Kafka
	- Amazon Kinesis (partially)
- **DB Sources**
	- MySQL
	- PostgreSQL
	- MS SQLServer
	- Amazon Redshift
	- Elasticsearch
- **File Sources**
	- Avro
	- Parquet
	- CSV
	- Orc
	- JSON

_Obs: File sources currently support `file` and `s3` protocols_

## Currently supported output sources

- **Stream Sources**
	- Apache Kafka
	- Amazon Kinesis
- **DB Sources**
	- Amazon Redshift
	- Elasticsearch
- **File Sources**
	- Avro
	- Parquet
	- Orc

_Obs: File sources currently support `file` and `s3` protocols_

## Currently supported utility sources

- **DynamoDB**
- **ElasticSearch** (partially)
- **MySQL** (partially)

## Property guide

First of all, we must assume that **every** `.properties` file must contain 4 sections of properties:

- **Common properties**: meant for general properties needed by spark or miscellaneous stuff
- **Input properties**: meant for input configuration
- **Output properties**:  meant for output configuration
- **Utilities properties**: meant for metadata, offset, etc

Every different stack will have its own set of properties, as needed by the library, or some specific properties needed by the engine, for example.

_Obs: **Italic-bold** text means that this property must be set in any case_
_Obs 2: If the property has a `[$type]`, this type must be respected_

### Common properties

_**spark.master**_ - Master node in which job will be executed.
_**spark.application**_ - Name of the job inside yarn.

### Input properties

This section will be divided by types and engine/format of inputs, and each property it needs.

**[STREAM] - Apache Kafka**
**input.type** - Type of input, in this case: `stream`.
**input.source** - Name of the stream topic to be used as metadata/output.
**input.engine** - Stream engine, in this case: `kafka`.
**input.topics** - [List] List of topics consumed, separated by `,`.
**input.schemaPath** - Path where the schema file is at.
**input.delimiter** - [Char] Delimiter used to separate stream row values.
**input.keyDeserializer** - Complete reference to the key deserializer class (i.e. `KafkaAvroDeserializer`).
**input.valueDeserializer** - Complete reference to the value deserializer class (i.e. `KafkaAvroDeserializer`).
**input.bootstrapServers** - Kafka Bootstrap servers.
**input.groupId**- Kafka Group Id.
**input.autoOffsetReset** - Kafka auto offreset.
**input.enableAutoCommit**- Kafka auto commit.
**input.securityProtocol** - [Boolean] If kafka has security protocol.
**input.sslTruststore.location** - **ONLY IF SECURITY PROTOCOL IS TRUE** Kafka SSL truststore location.
**input.sslTruststore.password** - **ONLY IF SECURITY PROTOCOL IS TRUE** Kafka SSL truststore password.
**input.sslKeystore.location** - **ONLY IF SECURITY PROTOCOL IS TRUE** Kafka SSL keystore location.
**input.sslKeystore.password** - **ONLY IF SECURITY PROTOCOL IS TRUE** Kafka SSL keystore password.
**input.sslKey.password** - **ONLY IF SECURITY PROTOCOL IS TRUE** Kafka SSL key password.

**[STREAM] - Apache Kinesis**
**input.type** - Type of input, in this case: `stream`.
**input.source** - Name of the stream topic to be used as metadata/output.
**input.engine** - Stream engine, in this case: `kinesis`.
**input.topics** - [List] List of topics consumed, separated by `,`.
**input.schemaPath** - Path where the schema file is at.
**input.delimiter** - [Char] Delimiter used to separate stream row values.
**input.stream** - Kinesis stream name.
**input.region** - Region at where kinesis stream is.
**input.endpoint** - Endpoint at where kinesis listens to.

**[DATABASE] - MySQL**
**input.type** - Type of input, in this case: `db`.
**input.source** - The name of the database.
**input.engine** - The database engine, in this case: `mysql`.
**input.location** - Name of the table consumed by the job.
**input.offsetField** - Name of the field used as offset by the job to gather information.
**input.url** - Url to mysql server.
**input.user** - User to access mysql server.
**input.pass** - Password to access mysql server.

**[DATABASE] - MS SQL**
**input.type** - Type of input, in this case: `db`.
**input.source** - Name of the database.
**input.engine** - Database engine, in this case: `mssql`.
**input.location** - Name of the table consumed by the job.
**input.offsetField** - Name of the field used as offset by the job to gather information.
**input.url** - Full connection string to mssql server.

**[DATABASE] - PostgreSQL**
**input.type** - Type of input, in this case: `db`.
**input.source** - Name of the database.
**input.engine** - Database engine, in this case: `pgsql`.
**input.location** - Name of the table consumed by the job.
**input.offsetField** - Name of the field used as offset by the job to gather information.
**input.url** - Url to postgres server.
**input.user** - User to access postgres server.
**input.pass** - Password to access postgres server.

**[DATABASE] - Amazon Redshift**
**input.type** - Type of input, in this case: `db`.
**input.source** - Name of the database.
**input.engine** - Database engine, in this case: `redshift`.
**input.location** - Name of the table consumed by the job.
**input.offsetField** - Name of the field used as offset by the job to gather information.
**input.url** - Full connection string to redshift server.
**input.tempDir** - Full s3 path string to temporary folder used by spark to read from redshift.
**awsAccessKeyId** - Access Key Id to aws account where the temporary folder is at.
**awsScretAccessKeyId** - Secret Access Key to aws account where the temporary folder is at.

**[DATABASE] - Elasticsearch**
**input.type** - Type of input, in this case: `db`.
**input.source** - Name of the database.
**input.engine** - Database engine, in this case: `es`.
**input.location** - Name of the index consumed by the job.
**input.offsetField** - Name of the field used as offset by the job to gather information.
**input.host** - Url to elasticsearch server.
**input.port** - Port at where elasticsearch server listens to.

**[FILE] - Avro**
**input.type** - Type of input, in this case: `file`.
**input.source** - Name of the file source.
**input.format** - Format of consumed files, in this case: `avro`.
**transformer** - Bean reference to the transformer class (i.e. `typeValidationAndMetadataTransformer`).

**[FILE] - CSV**
**input.type** - Type of input, in this case: `file`.
**input.source** - Name of the file source.
**input.format** - Format of consumed files, in this case: `csv`.
**transformer** - Bean reference to the transformer class (i.e. `typeValidationAndMetadataTransformer`).
**input.dms** - [Boolean] **True** if the `.csv` comes from Amazon DMS.
**input.delimiter** - [Char] The delimiter used to separate values in `.csv` files.

**[FILE] - JSON**
**input.type** - Type of input, in this case: `file`.
**input.source** - Name of the file source.
**input.format** - Format of consumed files, in this case: `json`.
**transformer** - Bean reference to the transformer class (i.e. `typeValidationAndMetadataTransformer`).
**input.multiLine** - [Boolean] **True** if json object occupies more than a single line or if the file is a NDJSON/JSONL.

**[FILE] - Orc**
**input.type** - Type of input, in this case: `file`.
**input.source** - Name of the file source.
**input.format** - Format of consumed files, in this case: `orc`.
**transformer** - Bean reference to the transformer class (i.e. `typeValidationAndMetadataTransformer`).

**[FILE] - Parquet**
**input.type** - Type of input, in this case: `file`.
**input.source** - Name of the file source.
**input.format** - Format of consumed files, in this case: `parquet`.
**transformer** - Bean reference to the transformer class (i.e. `typeValidationAndMetadataTransformer`).

### Output properties

This section will be divided by types and engine/format of inputs, and each property it needs.

**[STREAM] - Apache Kafka**
**output.type** - Type of output, in this case: `stream`.
**output.engine** - Stream engine, in this case: `kafka`.
**output.keyDeserializer** - Complete reference to the key deserializer class (i.e. `KafkaAvroDeserializer`).
**output.valueDeserializer** - Complete reference to the value deserializer class (i.e. `KafkaAvroDeserializer`).
**output.bootstrapServers** - Kafka Bootstrap servers.
**output.acks** - Kafka acks configuration.
**output.retries** - Kafka retry configuration.
**output.batchSize** - Kafka batch size configuration.
**output.lingerMs** - Kafka linger configuration.
**output.bufferMemory** - Kafka buffer configuration.
**output.securityProtocol** - [Boolean] If kafka has security protocol.
**output.sslTruststore.location** - **ONLY IF SECURITY PROTOCOL IS TRUE** Kafka SSL truststore location.
**output.sslTruststore.password** - **ONLY IF SECURITY PROTOCOL IS TRUE** Kafka SSL truststore password.
**output.sslKeystore.location** - **ONLY IF SECURITY PROTOCOL IS TRUE** Kafka SSL keystore location.
**output.sslKeystore.password** - **ONLY IF SECURITY PROTOCOL IS TRUE** Kafka SSL keystore password.
**output.sslKey.password** - **ONLY IF SECURITY PROTOCOL IS TRUE** Kafka SSL key password.

**[STREAM] - Apache Kinesis**
**output.type** - Type of output, in this case: `stream`.
**output.engine** - Stream engine, in this case: `kinesis`.
**output.region** - Region at where kinesis stream is.
**output.partitionKey** - Partition key to hit kinesis.

**[DATABASE] - Amazon Redshift**
**output.type** - Type of output, in this case: `db`.
**output.location** - Name of the table where the data is going to be stored.
**output.engine** - Database engine, in this case: `redshift`.
**output.url** - Full connection string to redshift server.
**output.tempDir** - Full s3 path string to temporary folder used by spark to read from redshift.
**awsAccessKeyId** - Access Key Id to aws account where the temporary folder is at.
**awsScretAccessKeyId** - Secret Access Key to aws account where the temporary folder is at.

**[DATABASE] - Elasticsearch**
**output.type** - Type of output, in this case: `db`.
**output.location** - Name of the index where the data is going to be stored.
**output.engine** - Database engine, in this case: `es`.
**output.host** - Url to elasticsearch server.
**output.port** - Port at where elasticsearch server listens to.

**[FILE] - Avro**
**output.type** - Type of output, in this case: `file`.
**output.format** - Format of generated files, in this case: `avro`.
**output.bucket** - Bucket at where generated files are going to be stored.
**output.protocol** - Protocol used to store generated files. Currently accepts: `file` and `s3`.

**[FILE] - Orc**
**output.type** - Type of output, in this case: `file`.
**output.format** - Format of generated files, in this case: `orc`.
**output.bucket** - Bucket at where generated files are going to be stored.
**output.protocol** - Protocol used to store generated files. Currently accepts: `file` and `s3`.

**[FILE] - Parquet**
**output.type** - Type of output, in this case: `file`.
**output.format** - Format of generated files, in this case: `parquet`.
**output.bucket** - Bucket at where generated files are going to be stored.
**output.protocol** - Protocol used to store generated files. Currently accepts: `file` and `s3`.

### Utilities properties

_**metadata**_ - [Boolean] If you have file inputs, this option **MUST** be set as true, otherwise it  **MUST** be set as false.
_**offset**_ - [Boolean] If you have database inputs, this option **MUST** be set as true, otherwise it  **MUST** be set as false.

**utils.bucket** - Bucket at where utilities files (like schema, entities list) are stored.
**olapEntitiesList.path (experimental)** - Path where the olap entities list file is at.
**entitiesList.path (experimental)** - Path where the entities list file is at.
**vault.uri** - URI to access vault.

**[METADATA - Dynamo]**
**metadata.engine** - Engine responsible for storing metadata, in this case `dynamo`.
**metadata.table** - Table at DynamoDB at where metadata is stored.
**metadata.index** - Index at DynamoDB which metadata table uses.

**[OFFSET - Dynamo]**
**offset.engine** - Engine responsible for storing offset data, in this case: `dynamo`.
**metadata.table** - Table at DynamoDB at where offset data is stored.


## Suggested reading

- [Spring Framework](https://docs.spring.io/spring-framework/docs/current/spring-framework-reference/core.html)
- [Apache Spark 2.3.0](https://spark.apache.org/docs/2.3.0/)
- [Amazon Redshift](https://aws.amazon.com/documentation/redshift/)
- [Amazon Kinesis](https://aws.amazon.com/documentation/kinesis/)

## Built with

- **Spring Framework** - Dependency Injection
- **Apache Spark 2.3.0** - Data processing framework
- **JUnit** - Testing

## Author

[**Flavio Teixeira**](https://github.com/fmteixeira)

[![Rivendel Tecnologia](http://res.cloudinary.com/hrscywv4p/image/upload/c_limit,fl_lossy,h_1440,w_720,f_auto,q_auto/v1/782436/Screen_Shot_2016-03-05_at_17.45.10_c9l4og.png)](http://www.rivendel.com.br)
