# PostgreSQL SQL Dialect User Guide

[PostgreSQL](https://www.postgresql.org/) is an open-source  Relational Database Management System (RDBMS).

## Uploading the JDBC Driver to Exasol BucketFS

1. Download the [PostgreSQL JDBC driver](https://jdbc.postgresql.org/).

    Driver version 42.2.6 or later is recommended if you want to establish a TLS-secured connection.
2. Upload the driver to BucketFS, see [BucketFS documentation](https://docs.exasol.com/db/latest/administration/on-premise/bucketfs/accessfiles.htm).

    Hint: Put the driver into folder `default/drivers/jdbc/` to register it for [ExaLoader](#registering-the-jdbc-driver-for-exaloader), too.

## Registering the JDBC driver for ExaLoader

In order to enable the ExaLoader to fetch data from the external database you must register the driver for ExaLoader as described in the [Installation procedure for JDBC drivers](https://github.com/exasol/docker-db/#installing-custom-jdbc-drivers).
1. ExaLoader expects the driver in BucketFS folder `default/drivers/jdbc`.<br />
If you uploaded the driver for UDF to a different folder, then you need to [upload](#uploading-the-jdbc-driver-to-exasol-bucketfs) the driver again.
2. Additionally  you need to create file `settings.cfg` and [upload](#uploading-the-jdbc-driver-to-exasol-bucketfs) it to the same folder in BucketFS:

```
DRIVERNAME=POSTGRES_JDBC_DRIVER
JAR=<jar file containing the jdbc driver>
DRIVERMAIN=org.postgresql.Driver
PREFIX=jdbc:postgresql:
FETCHSIZE=100000
INSERTSIZE=-1
```

| Variable | Description |
|----------|-------------|
| `<jar file containing the jdbc driver>` | E.g. `postgresql-42.4.2.jar` |

## Installing the Adapter Script

[Upload](https://docs.exasol.com/db/latest/administration/on-premise/bucketfs/accessfiles.htm) the latest available release of [PostgreSQL Virtual Schema JDBC Adapter](https://github.com/exasol/postgresql-virtual-schema/releases) to Bucket FS.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA ADAPTER;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.

```sql
--/
CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/<BFS service>/<bucket>/virtual-schema-dist-13.0.0-postgresql-3.1.1.jar;
  %jar /buckets/<BFS service>/<bucket>/postgresql-<postgresql-driver-version>.jar;
/
```

## Defining a Named Connection

Define the connection to the PostgreSQL database as shown below. We recommend using TLS to secure the connection.

```sql
CREATE OR REPLACE CONNECTION POSTGRESQL_CONNECTION
TO 'jdbc:postgresql://<host>:<port>/<database name>?ssl=true&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory'
USER '<user>'
IDENTIFIED BY '<password>';
```

If your setup does not support SSL then simply remove suffix `?ssl=true&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory`.


| Variable | Description |
|----------|-------------|
| `<host>` | Hostname or ip address of the machine hosting you PostgreSQL database. |
| `<port>` | Port of the PostgreSQL database, default is `5432`, see also [Developer's guide](../developers_guide/developers_guide.md#finding-out-the-port-of-a-postgresql-database-installation). |
| `<schema name>` | Name of the database schema you want to use in the PostgreSQL database. |

See also [Making PostgreSQL Service Listen to External Connections](../developers_guide/developers_guide.md#making-postgresql-service-listen-to-external-connections) in the Developer's guide.

## Creating a Virtual Schema

Use the following SQL command in Exasol database to create a PostgreSQL Virtual Schema:

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
  USING ADAPTER.JDBC_ADAPTER
  WITH
  CATALOG_NAME = '<catalog name>'
  SCHEMA_NAME = '<schema name>'
  CONNECTION_NAME = 'POSTGRESQL_CONNECTION';
```

| Variable | Description |
|----------|-------------|
| `<virtual schema name>` | Name of the virtual schema you want to use. |
| `<catalog name>` | Name of the catalog, usually equivalent to the name of the PostgreSQL database. |
| `<schema name>` | Name of the database schema you want to use in the PostgreSQL database. |

See also section [Remote logging](../developers_guide/developers_guide.md#remote-logging) in the developers guide.

For additional parameters coming from the base library see also [Adapter Properties for JDBC-Based Virtual Schemas](https://github.com/exasol/virtual-schema-common-jdbc#adapter-properties-for-jdbc-based-virtual-schemas).

## PostgreSQL Identifiers

In contrast to Exasol, PostgreSQL does not treat identifiers as specified in the SQL standard. PostgreSQL folds unquoted identifiers to lower case instead of upper case. The adapter has two modes for handling this:

### Automatic Identifier conversion

This is the default mode for handling identifiers, but identifier conversion can also be set explicitly using the following property:

```sql
ALTER VIRTUAL SCHEMA <virtual schema name> SET POSTGRESQL_IDENTIFIER_MAPPING = 'CONVERT_TO_UPPER';
```

In this mode you do not have to care about identifier handling. Everything will work as expected out of the box as long as you **do not use quoted identifiers** (in the PostgreSQL Schema as well as in the Exasol Virtual Schema). More specifically everything will work as long as there are no identifiers in the PostgreSQL database that contain upper case characters. If that is the case an error is thrown when creating or refreshing the virtual schema.
Regardless of this, you can create or refresh the virtual schema by specifying the adapter to ignore this particular error as shown below:

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
  USING ADAPTER.JDBC_ADAPTER
  WITH
  CATALOG_NAME = '<catalog name>'
  SCHEMA_NAME = '<schema name>'
  CONNECTION_NAME = 'POSTGRESQL_CONNECTION'
  IGNORE_ERRORS = 'POSTGRESQL_UPPERCASE_TABLES';
```
You can also set this property to an existing virtual schema:

```sql
ALTER VIRTUAL SCHEMA postgres SET IGNORE_ERRORS = 'POSTGRESQL_UPPERCASE_TABLES';
```
However, you **will not be able to query the identifier containing the upper case character**. An error is thrown when querying the virtual table.

A best practice for this mode is: **never quote identifiers** (in the PostgreSQL Schema as well as in the Exasol Virtual Schema). This way everything works without having to change your queries.<br />
An alternative is to use the second mode for identifier handling (see below).

### PostgreSQL-like identifier handling

If you use quotes on the PostgreSQL side and have identifiers with uppercase characters, then it is recommended to use this mode. The PostgreSQL like identifier handling does no conversions but mirrors the PostgreSQL metadata as is. A small example to make this clear:
```sql
--PostgreSQL Schema
CREATE TABLE "MyTable"("Col1" VARCHAR(100));
CREATE TABLE MySecondTable(Col1 VARCHAR(100));
--PostgreSQL Queries
SELECT "Col1" FROM "MyTable";
SELECT Col1 FROM MySecondTable;
```
```sql
--Create Virtual Schema on EXASOL side
CREATE VIRTUAL SCHEMA <virtual schema name>
  USING ADAPTER.JDBC_ADAPTER
  WITH
  CATALOG_NAME = '<catalog name>'
  SCHEMA_NAME = '<schema name>'
  CONNECTION_NAME = 'POSTGRESQL_CONNECTION'
  POSTGRESQL_IDENTIFIER_MAPPING = 'PRESERVE_ORIGINAL_CASE';

-- Open Schema and see what tables are there
OPEN SCHEMA postgres;
SELECT * FROM CAT;
-- result -->
-- TABLE_NAME  TABLE_TYPE
-- ----------------------
-- MyTable       | TABLE
-- mysecondtable | TABLE
```

As you can see `MySecondTable` is displayed in lower case in the virtual schema catalog. This is exactly like it is on the PostgreSQL side, but since unquoted identifiers are folded differently in PostgreSQL you cannot query the table like you did in PostgreSQL:

```sql
--Querying the virtual schema
--> this works
SELECT "Col1" FROM postgres."MyTable";

--> this does not work
SELECT Col1 FROM postgres.MySecondTable;
--> Error:
--  [Code: 0, SQL State: 42000]  object "POSTGRES"."MYSECONDTABLE" not found [line 1, column 18]

--> this works
SELECT "col1" FROM postgres."mysecondtable";
```

Unquoted identifiers are converted to lowercase on the PostgreSQL side, and since there is no catalog conversion these identifiers are also lowercase in Exasol. To query a lowercase identifier you must use quotes in Exasol, because everything that is unquoted gets folded to uppercase.

A best practice for this mode is: **always quote identifiers** (in the PostgreSQL Schema as well as in the Exasol Virtual Schema). This way everything works without having to change your queries.

## Data Types Conversion

| PostgreSQL Data Type     | Supported    | Converted Exasol Data Type | Known limitations                                                         |
|--------------------------|--------------|---------------------------|---------------------------------------------------------------------------|
| BIGINT                   | ✓            | DECIMAL(19,0)             |                                                                           |
| BIGSERIAL                | ✓            | DECIMAL(19,0)             |                                                                           |
| BIT                      | ✓            | BOOLEAN                   |                                                                           |
| BIT VARYING              | ✓            | VARCHAR(5)                |                                                                           |
| BOX                      | ✓            | VARCHAR(2000000)          |                                                                           |
| BYTEA                    | ✓            | VARCHAR(2000000)          |                                                                           |
| BOOLEAN                  | ✓            | BOOLEAN                   |                                                                           |
| CHARACTER                | ✓            | CHAR                      |                                                                           |
| CHARACTER VARYING        | ✓            | VARCHAR                   |                                                                           |
| CIDR                     | ✓            | VARCHAR(2000000)          |                                                                           |
| CIRCLE                   | ✓            | VARCHAR(2000000)          |                                                                           |
| DATE                     | ✓            | DATE                      |                                                                           |
| DOUBLE PRECISION         | ✓            | DOUBLE                    |                                                                           |
| INET                     | ✓            | VARCHAR(2000000)          |                                                                           |
| INTEGER                  | ✓            | DECIMAL(10,0)             |                                                                           |
| INTERVAL                 | ✓            | VARCHAR(2000000)          |                                                                           |
| JSON                     | ✓            | VARCHAR(2000000)          |                                                                           |
| JSONB                    | ✓            | VARCHAR(2000000)          |                                                                           |
| LINE                     | ✓            | VARCHAR(2000000)          |                                                                           |
| LSEG                     | ✓            | VARCHAR(2000000)          |                                                                           |
| MACADDR                  | ✓            | VARCHAR(2000000)          |                                                                           |
| MONEY                    | ✓            | DOUBLE                    |                                                                           |
| NUMERIC                  | ✓            | VARCHAR(2000000)          | Stored in Exasol as VARCHAR, because PostgreSQL NUMERIC values can exceed  Exasol Decimal limit which makes it impossible to use Virtual Schemas. |
| PATH                     | ✓            | VARCHAR(2000000)          |                                                                           |
| POINT                    | ✓            | VARCHAR(2000000)          |                                                                           |
| POLYGON                  | ✓            | VARCHAR(2000000)          |                                                                           |
| REAL                     | ✓            | DOUBLE                    |                                                                           |
| SMALLINT                 | ✓            | DECIMAL(5,0)              |                                                                           |
| SMALLSERIAL              | ? (untested) |                           |                                                                           |
| SERIAL                   | ? (untested) |                           |                                                                           |
| TEXT                     | ✓            | VARCHAR(2000000)          |                                                                           |
| TIME                     | ✓            | VARCHAR(2000000)          |                                                                           |
| TIME WITH TIME ZONE      | ✓            | VARCHAR(2000000)          |                                                                           |
| TIMESTAMP                | ✓            | TIMESTAMP                 |                                                                           |
| TIMESTAMP WITH TIME ZONE | ✓            | TIMESTAMP                 |                                                                           |
| TSQUERY                  | ✓            | VARCHAR(2000000)          |                                                                           |
| TSVECTOR                 | ✓            | VARCHAR(2000000)          |                                                                           |
| UUID                     | ✓            | VARCHAR(2000000)          |                                                                           |
| XML                      | ✓            | VARCHAR(2000000)          |                                                                           |
