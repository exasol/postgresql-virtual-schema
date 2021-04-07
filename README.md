# PostgreSQL Virtual Schema

[![Build Status](https://api.travis-ci.com/exasol/postgresql-virtual-schema.svg?branch=main)](https://travis-ci.com/exasol/postgresql-virtual-schema)

SonarCloud results:

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Apostgresql-virtual-schema&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.exasol%3Apostgresql-virtual-schema)

[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Apostgresql-virtual-schema&metric=security_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Apostgresql-virtual-schema)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Apostgresql-virtual-schema&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Apostgresql-virtual-schema)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Apostgresql-virtual-schema&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=com.exasol%3Apostgresql-virtual-schema)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Apostgresql-virtual-schema&metric=sqale_index)](https://sonarcloud.io/dashboard?id=com.exasol%3Apostgresql-virtual-schema)

[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Apostgresql-virtual-schema&metric=code_smells)](https://sonarcloud.io/dashboard?id=com.exasol%3Apostgresql-virtual-schema)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Apostgresql-virtual-schema&metric=coverage)](https://sonarcloud.io/dashboard?id=com.exasol%3Apostgresql-virtual-schema)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Apostgresql-virtual-schema&metric=duplicated_lines_density)](https://sonarcloud.io/dashboard?id=com.exasol%3Apostgresql-virtual-schema)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=com.exasol%3Apostgresql-virtual-schema&metric=ncloc)](https://sonarcloud.io/dashboard?id=com.exasol%3Apostgresql-virtual-schema)

# Overview

The **PostgreSQL Virtual Schema** provides an abstraction layer that makes an external [PostgreSQL](https://www.postgresql.org/) database accessible from an Exasol database through regular SQL commands. The contents of the external MySQL database are mapped to virtual tables which look like and can be queried as any regular Exasol table.

If you want to set up a Virtual Schema for a different database system, please head over to the [Virtual Schemas Repository][virtual-schemas].

## Features

* Access a PostgreSQL database using a Virtual Schema.
* Access PostgreSQL compatible databases:
    * [Greenplum](https://greenplum.org/)
    * [AWS Aurora](https://aws.amazon.com/de/rds/aurora/)

## Table of Contents

### Information for Users

* [PostgreSQL dialect User Guide](doc/user_guide/postgresql_user_guide.md)
* [General Virtual Schema User Guide][user-guide]
* [List of supported capabilities](doc/generated/capabilities.md)
* [Changelog](doc/changes/changelog.md)

Find all the documentation in the [Virtual Schemas project][vs-doc].

## Information for Developers

* [Virtual Schema API Documentation][vs-api]

### Run Time Dependencies

Running the Virtual Schema requires a Java Runtime version 11 or later.

| Dependency                                                         | Purpose                                                | License                       |
|--------------------------------------------------------------------|--------------------------------------------------------|-------------------------------|
| [Exasol Virtual Schema JDBC][virtual-schema-common-jdbc]           | Common JDBC functions for Virtual Schemas adapters     | MIT License                   |
| [PostgreSQL JDBC Driver][postgresql-jdbc-driver]                   | JDBC driver for PostgreSQL database                    | BSD-2-Clause License          |

### Test Dependencies

| Dependency                                                         | Purpose                                                | License                       |
|--------------------------------------------------------------------|--------------------------------------------------------|-------------------------------|
| [Apache Maven](https://maven.apache.org/)                          | Build tool                                             | Apache License 2.0            |
| [Exasol Testcontainers][exasol-testcontainers]                     | Exasol extension for the Testcontainers framework      | MIT License                   |
| [Java Hamcrest](http://hamcrest.org/JavaHamcrest/)                 | Checking for conditions in code via matchers           | BSD License                   |
| [JUnit](https://junit.org/junit5)                                  | Unit testing framework                                 | Eclipse Public License 1.0    |
| [Mockito](http://site.mockito.org/)                                | Mocking framework                                      | MIT License                   |
| [Testcontainers](https://www.testcontainers.org/)                  | Container-based integration tests                      | MIT License                   |
| [Test Database Builder][test-db-builder]                           | Fluent database interfaces for testing                 | MIT License                   |
| [Test Database Builder][test-db-builder]                           | Fluent database interfaces for testing                 | MIT License                   |
| [Java-Markdown-Generator][Java-Markdown-Generator]                 | Markdown table creation                                | MIT License                   |
| [Autogenerated Resource Verifier][Autogenerated-Resource-Verifier] | Verify autogenerated documentation                     | MIT License                   |
| [Virtual Schema integration tests][vs-shared-integration-tests]    | Integration tests for all virtual Schemas              | MIT License                   |

### Maven Plug-ins

| Plug-in                                                            | Purpose                                                | License                       |
|--------------------------------------------------------------------|--------------------------------------------------------|-------------------------------|
| [Maven Surefire Plugin][maven-surefire-plugin]                     | Unit testing                                           | Apache License 2.0            |
| [Maven Jacoco Plugin][maven-jacoco-plugin]                         | Code coverage metering                                 | Eclipse Public License 2.0    |
| [Maven Compiler Plugin][maven-compiler-plugin]                     | Setting required Java version                          | Apache License 2.0            |
| [Maven Assembly Plugin][maven-assembly-plugin]                     | Creating JAR                                           | Apache License 2.0            |
| [Maven Failsafe Plugin][maven-failsafe-plugin]                     | Integration testing                                    | Apache License 2.0            |
| [Sonatype OSS Index Maven Plugin][sonatype-oss-index-maven-plugin] | Checking Dependencies Vulnerability                    | ASL2                          |
| [Versions Maven Plugin][versions-maven-plugin]                     | Checking if dependencies updates are available         | Apache License 2.0            |
| [Maven Enforcer Plugin][maven-enforcer-plugin]                     | Controlling environment constants                      | Apache License 2.0            |
| [Artifact Reference Checker Plugin][artifact-ref-checker-plugin]   | Check if artifact is referenced with correct version   | MIT License                   |
| [Project Keeper Maven Plugin][project-keeper-maven-plugin]         | Checking project structure                             | MIT License                   |

<!-- @formatter:off -->
[maven-surefire-plugin]: https://maven.apache.org/surefire/maven-surefire-plugin/
[maven-jacoco-plugin]: https://www.eclemma.org/jacoco/trunk/doc/maven.html
[maven-compiler-plugin]: https://maven.apache.org/plugins/maven-compiler-plugin/
[maven-assembly-plugin]: https://maven.apache.org/plugins/maven-assembly-plugin/
[maven-failsafe-plugin]: https://maven.apache.org/surefire/maven-failsafe-plugin/
[sonatype-oss-index-maven-plugin]: https://sonatype.github.io/ossindex-maven/maven-plugin/
[versions-maven-plugin]: https://www.mojohaus.org/versions-maven-plugin/
[maven-enforcer-plugin]: http://maven.apache.org/enforcer/maven-enforcer-plugin/
[artifact-ref-checker-plugin]: https://github.com/exasol/artifact-reference-checker-maven-plugin
[project-keeper-maven-plugin]: https://github.com/exasol/project-keeper-maven-plugin
[postgresql-jdbc-driver]: https://jdbc.postgresql.org/
[test-db-builder]: https://github.com/exasol/test-db-builder/
[virtual-schema-common-jdbc]: https://github.com/exasol/virtual-schema-common-jdbc
[exasol-testcontainers]: https://github.com/exasol/exasol-testcontainers
[user-guide]: https://docs.exasol.com/database_concepts/virtual_schemas.htm
[virtual-schemas]: https://github.com/exasol/virtual-schemas
[vs-api]: https://github.com/exasol/virtual-schema-common-java/blob/master/doc/development/api/virtual_schema_api.md
[vs-doc]: https://github.com/exasol/virtual-schemas/tree/master/doc
[Java-Markdown-Generator]: https://github.com/Steppschuh/Java-Markdown-Generator
[Autogenerated-Resource-Verifier]: https://github.com/exasol/autogenerated-resource-verifier-java
[vs-shared-integration-tests]: https://github.com/exasol/virtual-schema-shared-integration-tests/
<!-- @formatter:on -->