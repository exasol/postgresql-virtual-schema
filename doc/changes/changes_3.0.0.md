# Virtual Schema for PostgreSQL 3.0.0, released 2024-02-28

Code name: Char set is always `utf-8`, deprecated IMPORT_DATA_TYPES `FROM_RESULT_SET` value


## Summary

The behaviour when it comes to character sets is now simplified,
The target char set is now always UTF-8.
The `IMPORT_DATA_TYPES` property (and value `FROM_RESULT_SET`) are now deprecated (change in vs-common-jdbc):
An exception will be thrown when users use `FROM_RESULT_SET`. The exception message warns the user that the value is no longer supported and the property itself is also deprecated.

Various broken scalar time-related extraction functions for dates and timestamps in the virtual schema are now fixed: `year`,`month`,`day`,`hour`,`minute`,`second`.

Scalar `division` (`/`) which was broken in some cases now also works correctly.

Tests for `current_schema` are currently disabled, this is because of a discovered compiler bug: https://github.com/exasol/postgresql-virtual-schema/issues/79 . 
These tests will be re-evaluated later when there is more clarity about this issue.

We also updated dependencies and resolved the following 2 CVEs in test dependency `org.apache.commons:commons-compress`:
- CVE-2024-26308
- CVE-2024-25710
We also updated dependencies and resolved the following CVE in test dependency `org.postgresql:postgresql:`:
- CVE-2024-1597

## Features

- #68 : Update tests to V8 VSPG refactoring

## Security

- #78 : Fix vulnerabilities in org.postgresql:postgresql:jar:42.6.0:compile & org.apache.commons:commons-compress:jar:1.24.0:test

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:virtual-schema-common-jdbc:11.0.2` to `12.0.0`
* Updated `org.postgresql:postgresql:42.6.0` to `42.7.2`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:6.6.2` to `7.0.1`
* Updated `com.exasol:hamcrest-resultset-matcher:1.6.1` to `1.6.4`
* Updated `com.exasol:test-db-builder-java:3.5.1` to `3.5.3`
* Updated `com.exasol:virtual-schema-common-jdbc:11.0.2` to `12.0.0`
* Updated `com.exasol:virtual-schema-shared-integration-tests:2.2.5` to `3.0.0`
* Added `org.jacoco:org.jacoco.agent:0.8.11`
* Updated `org.junit.jupiter:junit-jupiter:5.10.0` to `5.10.1`
* Updated `org.mockito:mockito-junit-jupiter:5.5.0` to `5.10.0`
* Updated `org.testcontainers:junit-jupiter:1.19.0` to `1.19.4`
* Updated `org.testcontainers:postgresql:1.19.0` to `1.19.4`

### Plugin Dependency Updates

* Updated `com.exasol:error-code-crawler-maven-plugin:1.3.0` to `2.0.0`
* Updated `com.exasol:project-keeper-maven-plugin:2.9.12` to `4.0.0`
* Updated `org.apache.maven.plugins:maven-compiler-plugin:3.11.0` to `3.12.1`
* Updated `org.apache.maven.plugins:maven-dependency-plugin:2.8` to `3.6.1`
* Updated `org.apache.maven.plugins:maven-enforcer-plugin:3.4.0` to `3.4.1`
* Updated `org.apache.maven.plugins:maven-failsafe-plugin:3.1.2` to `3.2.5`
* Updated `org.apache.maven.plugins:maven-surefire-plugin:3.1.2` to `3.2.5`
* Added `org.apache.maven.plugins:maven-toolchains-plugin:3.1.0`
* Updated `org.codehaus.mojo:flatten-maven-plugin:1.5.0` to `1.6.0`
* Updated `org.codehaus.mojo:versions-maven-plugin:2.16.0` to `2.16.2`
* Updated `org.jacoco:jacoco-maven-plugin:0.8.10` to `0.8.11`
* Updated `org.sonarsource.scanner.maven:sonar-maven-plugin:3.9.1.2184` to `3.10.0.2594`
