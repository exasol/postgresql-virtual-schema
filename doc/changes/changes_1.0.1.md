# Virtual Schema for PostgreSQL 1.0.1, released 2020-??-??

Code name:

## Features / Enhancements

* #8: Add error codes to exceptions
* #15: Get driver for the integration tests from Maven
* #13: Added integration test for scalar functions

## Bugfixes

* #24: Fixed broken scalar functions: `ADD_DAYS`, `ADD_HOURS`, `ADD_MINUTES`, `ADD_MONTHS`, `ADD_SECONDS`, `ADD_WEEKS`, `ADD_YEARS`.

## Dependency updates

* Added `com.exasol:error-reporting-java:0.2.0`
* Added `org.apache.maven.plugins:maven-dependency-plugin:2.8`
* Updated `com.exasol:project-keeper-maven-plugin:0.4.1` to 0.4.2
* Updated `com.exasol:hamcrest-resultset-matcher:1.3.3` to 1.3.0
* Added `com.exasol:udf-debugging-java:0.3.0`
* Updated `com.exasol:hamcrest-resultset-matcher:1.3.0` to 1.4.0
* Updated `org.apache.maven.plugins.maven-surefire-plugin:3.0.0-M3` to 3.0.0-M5
* Updated `org.apache.maven.plugins.maven-failsafe-plugin:3.0.0-M3` to 3.0.0-M5
