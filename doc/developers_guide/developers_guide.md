# Developers Guide

This guide contains information for developers.

## Password for writing to BucketFS of your Exasol database

In case you are running Exasol in a docker container the following script helps you to get the password for writing to BucketFS of your Exasol database:

```shell
CONTAINER=<container id>
export BUCKETFS_PASSWORD=$(
  docker exec -it $CONTAINER \
  grep WritePass /exa/etc/EXAConf \
  | sed -e 's/.* = //' \
  | tr -d '\r' \
  | base64 -d)
```

## Remote Logging

When [creating a Virtual Schema](../user_guide/postgresql_user_guide.md#creating-a-virtual-schema) you can enable remote access to information logged by the virtual schema adapter see [Remote logging](https://docs.exasol.com/db/latest/database_concepts/virtual_schema/logging.htm).

Please note that remote logging
* imposes security risks on your system
* may affect the performance of your system
* should be used only for debugging and development purposes but not in productive scenarios

## Finding Out the Port of a PostgreSQL Database Installation

PostgreSQL default port is `5432`.<br />
To inquire port in other cases use

```shell
function hfield () { head -1 | sed -e 's/  */\t/g' | cut -f $1 ; }
SENDQ=$(ss -tl | grep postgresql | hfield 3)
ss -tln | grep $SENDQ | hfield 4
```

## Making PostgreSQL Service Listen to External Connections

In order to enable Exasol database to access your PostgreSQL database as a virtual schema you may need to make PostgreSQL Service listen to external connections.

See
* https://www.bigbinary.com/blog/configure-postgresql-to-allow-remote-connection
* https://dba.stackexchange.com/questions/83984/

Please note:
* Accepting external connections imposes security risks on your PostgreSQL database.
* In case you are not sure please contact your local IT security officer.
* The following steps are only suitable for limited experiments in a secure sandbox environment.

Use `sudo vi` to add the following line to file `/etc/postgresql/10/main/postgresql.conf`:
```
listen_addresses = '*'
```

Use `sudo vi`to add the following line to file `/etc/postgresql/10/main/pg_hba.conf`:
```
# TYPE DATABASE USER CIDR-ADDRESS  METHOD
host  all  all 0.0.0.0/0 md5
```

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

