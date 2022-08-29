# PostgreSQL SQL Dialect User Guide

[PostgreSQL](https://www.postgresql.org/) is an open-source  Relational Database Management System (RDBMS).

## Uploading the JDBC Driver to EXAOperation

First download the [PostgreSQL JDBC driver](https://jdbc.postgresql.org/).
Driver version 42.2.6 or later is recommended if you want to establish a TLS-secured connection.

1. [Create a bucket in BucketFS](https://docs.exasol.com/administration/on-premise/bucketfs/create_new_bucket_in_bucketfs_service.htm)
2. [Upload the driver to BucketFS](#uploading-a-file-to-bucketfs)

Hint: Put the driver to folder `default/drivers/jdbc/` to make it accessible to [ExaLoader](#configuring-exaloader) as well.

## Uploading a file to BucketFS
Get password for writing to BucketFS:
```shell
CONTAINER=<container id>
export BUCKETFS_PASSWORD=$(
  docker exec -it $CONTAINER \
  grep WritePass /exa/etc/EXAConf \
  | sed -e 's/.* = //' \
  | tr -d '\r' \
  | base64 -d)
```

Upload the driver file to BucketFS:
```shell
FILE=<name of the file to upload>
FOLDER=<folder in BucketFS>
curl -v -X PUT -T $FILE http://w:$BUCKETFS_PASSWORD@localhost:2580/$FOLDER/$FILE
```

| Variable | Description |
|----------|-------------|
| `<name of the file to upload>` | Name of the file you want to upload to BucketFS.|
| `<folder in BucketFS>` | The folder in BucketFS you want the file to upload to. Default root folder is `default`. |


## Installing the Adapter Script

[Upload](#uploading-a-file-to-bucketfs) the latest available release of [PostgreSQL Virtual Schema JDBC Adapter](https://github.com/exasol/postgresql-virtual-schema/releases) to Bucket FS.

Then create a schema to hold the adapter script.

```sql
CREATE SCHEMA ADAPTER;
```

The SQL statement below creates the adapter script, defines the Java class that serves as entry point and tells the UDF framework where to find the libraries (JAR files) for Virtual Schema and database driver.

```sql
--/
CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
  %scriptclass com.exasol.adapter.RequestDispatcher;
  %jar /buckets/<BFS service>/<bucket>/virtual-schema-dist-9.0.5-postgresql-2.0.3.jar;
  %jar /buckets/<BFS service>/<bucket>/postgresql-<postgresql-driver-version>.jar;
/
```

## Configuring ExaLoader

You must configure ExaLoader to enable it to actually import or *read* data from the external database, see https://github.com/exasol/docker-db/#installing-custom-jdbc-drivers.

ExaLoader expects the driver in BucketFS folder `default/drivers/jdbc`.<br />
If you uploaded the driver for UDF to a different folder, then you need to [upload](#uploading-a-file-to-bucketfs) the driver again.


Additionally  you need to create a file `settings.cfg` and [upload](#uploading-a-file-to-bucketfs) it to the same folder in BucketFS:

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
| `<jar file containing the jdbc driver>` | E.g. postgresql-42.4.2.jar |

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
| `<port>` | Port of the PostgreSQL database. Default port is `5432`, see also [Finding Out the Port of a PostgreSQL Database Installation](#finding-out-the-port-of-a-postgresql-database-installation). |
| `<schema name>` | Name of the database schema you want to use in the PostgreSQL database. |


## Finding Out the Port of a PostgreSQL Database Installation

PostgreSQL default port is `5432`.<br />
To inquire port in other cases use

```shell
function hfield () { head -1 | sed -e 's/  */\t/g' | cut -f $1 ; }
SENDQ=$(ss -tl | grep postgresql | hfield 3)
ss -tln | grep $SENDQ | hfield 4
```

## Making PostgreSQL Service Listen to External Connections

See
* https://www.bigbinary.com/blog/configure-postgresql-to-allow-remote-connection
* https://dba.stackexchange.com/questions/83984/

Use `sudo vi` to add the following line to file `/etc/postgresql/10/main/postgresql.conf`:
```
listen_addresses = '*'
```

Use `sudo vi`to add the following line to file `/etc/postgresql/10/main/pg_hba.conf`:
```
# TYPE DATABASE USER CIDR-ADDRESS  METHOD
host  all  all 0.0.0.0/0 md5
```

## Creating a Virtual Schema

Use the following SQL command in Exasol database to create a Postgres Virtual Schema:

```sql
CREATE VIRTUAL SCHEMA <virtual schema name>
	USING ADAPTER.JDBC_ADAPTER
	WITH
	CATALOG_NAME = '<catalog name>'
	SCHEMA_NAME = '<schema name>'
	CONNECTION_NAME = 'POSTGRESQL_CONNECTION'
	DEBUG_ADDRESS = '172.17.0.1:<port>'
	LOG_LEVEL = 'ALL'
	;
```

| Variable | Description |
|----------|-------------|
| `<virtual schema name>` | Name of the virtual schema you want to use. |
| `<catalog name>` | Name of the catalog, usally equivalent to the name of the PostgreSQL database. |
| `<schema name>` | Name of the database schema you want to use in the PostgreSQL database. |
| `<docker host>` | ip address of the docker container running the Exasol database. |
| `<port>`        | Port you want to use for logging debug information. |

## Display Log Output of Virtual Schema

When creating a virtual schema you can optionally configure it to write log output to a specific host and port.<br />
In order to display log output you can use the unix command `nc`.
The following command starts `nc` listening (`-l`), kept alive (`-k`) on port (`-p`)  `<port>`:
```
nc -lkp <port>
```

| Variable | Description |
|----------|-------------|
| `<port>` | Port `nc` should listen to. |


## First Steps With PostgreSQL

See also https://www3.ntu.edu.sg/home/ehchua/programming/sql/PostgreSQL_GetStarted.html.

Database clients: see https://wiki.postgresql.org/wiki/PostgreSQL_Clients.<br />
For the following examples we chose command line client `psql` included in default installation.

| Command | Description |
|---------|-------------|
| `sudo apt install postgresql` | Install PostgreSQL |
| `sudo -u postgres psql -c 'CREATE DATABASE mytest;'` | Create database named `mytest` |
| `sudo -u postgres createuser --superuser $USER` | Create a PostgreSQL user for you |
| `psql mytest` | Connect to database `mytest` using the current user |

Helpful commands in database client:

| Command | Comment |
| -------- | --------- |
| `SELECT version();` | Display version of installed database |
| `\h <command>` | help for command `<command>` |
| `\c <database-name>` | connect to database `<database-name>` |
| `\l` | list databases |
| `\dt` | list tables |
| `\password <user>` | set password for user `<user>` |
| `CREATE TABLE mytable columns (name VARCHAR, age INT);` | Create a table |

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
	IGNORE_ERRORS = 'POSTGRESQL_UPPERCASE_TABLES'
;
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
	POSTGRESQL_IDENTIFIER_MAPPING = 'PRESERVE_ORIGINAL_CASE'
;
-- Open Schema and see what tables are there
open schema postgres;
select * from cat;
-- result -->
-- TABLE_NAME	TABLE_TYPE
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

| PostgreSQL Data Type     | Supported    | Converted Exasol Data Type| Known limitations
|--------------------------|--------------|---------------------------|-------------------
| BIGINT                   | ✓            | DECIMAL(19,0)             |
| BIGSERIAL                | ✓            | DECIMAL(19,0)             |
| BIT                      | ✓            | BOOLEAN                   |
| BIT VARYING              | ✓            | VARCHAR(5)                |
| BOX                      | ✓            | VARCHAR(2000000)          |
| BYTEA                    | ✓            | VARCHAR(2000000)          |
| BOOLEAN                  | ✓            | BOOLEAN                   |
| CHARACTER                | ✓            | CHAR                      |
| CHARACTER VARYING        | ✓            | VARCHAR                   |
| CIDR                     | ✓            | VARCHAR(2000000)          |
| CIRCLE                   | ✓            | VARCHAR(2000000)          |
| DATE                     | ✓            | DATE                      |
| DOUBLE PRECISION         | ✓            | DOUBLE                    |
| INET                     | ✓            | VARCHAR(2000000)          |
| INTEGER                  | ✓            | DECIMAL(10,0)             |
| INTERVAL                 | ✓            | VARCHAR(2000000)          |
| JSON                     | ✓            | VARCHAR(2000000)          |
| JSONB                    | ✓            | VARCHAR(2000000)          |
| LINE                     | ✓            | VARCHAR(2000000)          |
| LSEG                     | ✓            | VARCHAR(2000000)          |
| MACADDR                  | ✓            | VARCHAR(2000000)          |
| MONEY                    | ✓            | DOUBLE                    |
| NUMERIC                  | ✓            | VARCHAR(2000000)          | Stored in Exasol as VARCHAR, because PostgreSQL NUMERIC values can exceed Exasol Decimal limit which makes it impossible to use Virtual Schemas.
| PATH                     | ✓            | VARCHAR(2000000)          |
| POINT                    | ✓            | VARCHAR(2000000)          |
| POLYGON                  | ✓            | VARCHAR(2000000)          |
| REAL                     | ✓            | DOUBLE                    |
| SMALLINT                 | ✓            | DECIMAL(5,0)              |
| SMALLSERIAL              | ? (untested) |                           |
| SERIAL                   | ? (untested) |                           |
| TEXT                     | ✓            | VARCHAR(2000000)          |
| TIME                     | ✓            | VARCHAR(2000000)          |
| TIME WITH TIME ZONE      | ✓            | VARCHAR(2000000)          |
| TIMESTAMP                | ✓            | TIMESTAMP                 |
| TIMESTAMP WITH TIME ZONE | ✓            | TIMESTAMP                 |
| TSQUERY                  | ✓            | VARCHAR(2000000)          |
| TSVECTOR                 | ✓            | VARCHAR(2000000)          |
| UUID                     | ✓            | VARCHAR(2000000)          |
| XML                      | ✓            | VARCHAR(2000000)          |

## Testing information

In the following matrix you find combinations of JDBC driver and dialect version that we tested.

| Virtual Schema Version | PostgreSQL Version | Driver Name            | Driver Version  |
|------------------------|--------------------|------------------------|-----------------|
| Latest                 | PostgreSQL 14.2    | PostgreSQL JDBC Driver |  42.4.2         |
