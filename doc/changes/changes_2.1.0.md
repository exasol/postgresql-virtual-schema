# Virtual Schema for PostgreSQL 2.1.0, released 2022-12-21

Code name: Result set Type Calculation Switch

## Summary

We updated `virtual-schema-common-jdbc` to version 10.1.0 in order to enable switching the calculation of the result set data types between calculated by Exasol (default) and inferred from JDBC. The default is more efficient, but some JDBC drivers are inconsistent when reporting character encodings. In those cases you can fall back to the old mechanism that infers the data types from the reported result set types. 

We also renamed error codes from PGVS to VSPG and removed the reference to the Exasol artifactory from the bill-of-materials file, because all dependencies are now available on Maven Central.

## Features

* #59: Renamed error codes from PGVS to VSPG.

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:virtual-schema-common-jdbc:10.0.1` to `10.1.0`
* Updated `org.postgresql:postgresql:42.5.0` to `42.5.1`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:6.2.0` to `6.4.1`
* Updated `com.exasol:test-db-builder-java:3.3.4` to `3.4.1`
* Updated `com.exasol:udf-debugging-java:0.6.4` to `0.6.5`
* Updated `com.exasol:virtual-schema-common-jdbc:10.0.1` to `10.1.0`
* Updated `com.exasol:virtual-schema-shared-integration-tests:2.2.2` to `2.2.3`
* Updated `org.mockito:mockito-junit-jupiter:4.8.0` to `4.10.0`
* Updated `org.testcontainers:junit-jupiter:1.17.3` to `1.17.6`
* Updated `org.testcontainers:postgresql:1.17.3` to `1.17.6`

### Plugin Dependency Updates

* Updated `com.exasol:artifact-reference-checker-maven-plugin:0.4.0` to `0.4.2`
* Updated `com.exasol:error-code-crawler-maven-plugin:1.1.1` to `1.2.1`
* Updated `com.exasol:project-keeper-maven-plugin:2.4.6` to `2.9.1`
* Updated `io.github.zlika:reproducible-build-maven-plugin:0.15` to `0.16`
* Updated `org.apache.maven.plugins:maven-assembly-plugin:3.3.0` to `3.4.2`
* Updated `org.apache.maven.plugins:maven-enforcer-plugin:3.0.0` to `3.1.0`
* Updated `org.apache.maven.plugins:maven-failsafe-plugin:3.0.0-M5` to `3.0.0-M7`
* Updated `org.apache.maven.plugins:maven-jar-plugin:3.2.2` to `3.3.0`
* Updated `org.apache.maven.plugins:maven-surefire-plugin:3.0.0-M5` to `3.0.0-M7`
* Updated `org.codehaus.mojo:exec-maven-plugin:3.0.0` to `3.1.0`
* Updated `org.codehaus.mojo:flatten-maven-plugin:1.2.7` to `1.3.0`
* Updated `org.codehaus.mojo:versions-maven-plugin:2.10.0` to `2.13.0`
