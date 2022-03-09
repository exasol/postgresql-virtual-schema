# Virtual Schema for PostgreSQL 2.0.2, released 2022-03-09

Code name: Upgrade Dependencies on top of 2.0.1

This release upgrades all dependencies to the latest versions.

## Bug Fixes

* #47: Updated PostgreSQL dependency to latest version
* #49: Updated PostgreSQL dependency to latest version

## Dependency Updates

### Compile Dependency Updates

* Updated `org.postgresql:postgresql:42.3.1` to `42.3.3`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:5.1.1` to `6.1.1`
* Updated `com.exasol:test-db-builder-java:3.2.1` to `3.3.1`
* Updated `com.exasol:udf-debugging-java:0.4.1` to `0.6.0`
* Updated `com.exasol:virtual-schema-shared-integration-tests:2.1.0` to `2.2.0`
* Updated `org.junit.jupiter:junit-jupiter:5.8.1` to `5.8.2`
* Updated `org.mockito:mockito-junit-jupiter:4.1.0` to `4.3.1`
* Updated `org.testcontainers:junit-jupiter:1.16.2` to `1.16.3`
* Updated `org.testcontainers:postgresql:1.16.2` to `1.16.3`

### Plugin Dependency Updates

* Updated `com.exasol:artifact-reference-checker-maven-plugin:0.4.0` to `0.4.1`
* Updated `com.exasol:error-code-crawler-maven-plugin:0.7.1` to `1.0.0`
* Updated `com.exasol:project-keeper-maven-plugin:1.3.2` to `2.0.0`
* Updated `io.github.zlika:reproducible-build-maven-plugin:0.13` to `0.15`
* Updated `org.apache.maven.plugins:maven-clean-plugin:2.5` to `3.1.0`
* Updated `org.apache.maven.plugins:maven-compiler-plugin:3.8.1` to `3.10.0`
* Updated `org.apache.maven.plugins:maven-dependency-plugin:2.8` to `3.2.0`
* Updated `org.apache.maven.plugins:maven-deploy-plugin:2.7` to `2.8.2`
* Updated `org.apache.maven.plugins:maven-install-plugin:2.4` to `2.5.2`
* Updated `org.apache.maven.plugins:maven-jar-plugin:3.2.0` to `3.2.2`
* Updated `org.apache.maven.plugins:maven-resources-plugin:2.6` to `3.2.0`
* Updated `org.apache.maven.plugins:maven-site-plugin:3.3` to `3.11.0`
* Added `org.codehaus.mojo:flatten-maven-plugin:1.2.7`
* Updated `org.codehaus.mojo:versions-maven-plugin:2.8.1` to `2.9.0`
* Updated `org.sonatype.ossindex.maven:ossindex-maven-plugin:3.1.0` to `3.2.0`
