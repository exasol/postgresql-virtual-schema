# Virtual Schema for PostgreSQL 4.0.0, released 2026-05-22

## Summary

This release introduces anonymous feature-usage telemetry through `telemetry-java`. See the [documentation](https://github.com/exasol/telemetry-java/blob/main/doc/app-user-guide.md) for details about the collected data and how to opt out.

## Breaking Change

Starting with this release, this Virtual Schema no longer supports Exasol 7.1. The supported versions are the current release and the LTS release line `2025.1.x`.

## Features

* #90: Added anonymous feature-usage tracking

## Bugfixes

* #89: Fixed the reported adapter version

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:error-reporting-java:1.0.1` to `1.0.2`
* Updated `com.exasol:virtual-schema-common-jdbc:13.0.0` to `14.0.2`
* Updated `org.postgresql:postgresql:42.7.7` to `42.7.11`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:7.1.7` to `7.3.0`
* Updated `com.exasol:hamcrest-resultset-matcher:1.7.1` to `1.7.2`
* Updated `com.exasol:test-db-builder-java:3.6.1` to `4.0.0`
* Updated `com.exasol:udf-debugging-java:0.6.16` to `0.6.18`
* Updated `com.exasol:virtual-schema-common-jdbc:13.0.0` to `14.0.2`
* Updated `com.exasol:virtual-schema-shared-integration-tests:3.0.1` to `3.0.2`
* Updated `org.jacoco:org.jacoco.agent:0.8.13` to `0.8.14`
* Updated `org.junit.jupiter:junit-jupiter:5.13.0` to `5.14.4`
* Updated `org.mockito:mockito-junit-jupiter:5.18.0` to `5.23.0`
* Updated `org.slf4j:slf4j-jdk14:2.0.17` to `2.0.18`
* Removed `org.testcontainers:junit-jupiter:1.21.1`
* Removed `org.testcontainers:postgresql:1.21.1`
* Added `org.testcontainers:testcontainers-junit-jupiter:2.0.5`
* Added `org.testcontainers:testcontainers-postgresql:2.0.5`

### Plugin Dependency Updates

* Updated `com.exasol:artifact-reference-checker-maven-plugin:0.4.3` to `0.4.4`
* Updated `com.exasol:error-code-crawler-maven-plugin:2.0.4` to `2.0.7`
* Updated `com.exasol:project-keeper-maven-plugin:5.2.3` to `5.6.2`
* Updated `com.exasol:quality-summarizer-maven-plugin:0.2.0` to `0.2.1`
* Updated `io.github.git-commit-id:git-commit-id-maven-plugin:9.0.1` to `10.0.0`
* Updated `org.apache.maven.plugins:maven-artifact-plugin:3.6.0` to `3.6.1`
* Updated `org.apache.maven.plugins:maven-assembly-plugin:3.7.1` to `3.8.0`
* Updated `org.apache.maven.plugins:maven-clean-plugin:3.4.1` to `3.5.0`
* Updated `org.apache.maven.plugins:maven-compiler-plugin:3.14.0` to `3.15.0`
* Updated `org.apache.maven.plugins:maven-dependency-plugin:3.8.1` to `3.10.0`
* Updated `org.apache.maven.plugins:maven-enforcer-plugin:3.5.0` to `3.6.2`
* Updated `org.apache.maven.plugins:maven-failsafe-plugin:3.5.3` to `3.5.5`
* Updated `org.apache.maven.plugins:maven-jar-plugin:3.4.2` to `3.5.0`
* Updated `org.apache.maven.plugins:maven-resources-plugin:3.3.1` to `3.5.0`
* Updated `org.apache.maven.plugins:maven-surefire-plugin:3.5.3` to `3.5.5`
* Updated `org.codehaus.mojo:exec-maven-plugin:3.5.0` to `3.6.3`
* Updated `org.codehaus.mojo:flatten-maven-plugin:1.7.0` to `1.7.3`
* Updated `org.codehaus.mojo:versions-maven-plugin:2.18.0` to `2.21.0`
* Updated `org.jacoco:jacoco-maven-plugin:0.8.13` to `0.8.14`
* Updated `org.sonarsource.scanner.maven:sonar-maven-plugin:5.1.0.4751` to `5.5.0.6356`
