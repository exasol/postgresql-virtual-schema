# Virtual Schema for PostgreSQL 2.2.1, released 2023-07-05

Code name: Update Documentation and Dependencies

## Summary

This release adds a reference to common adapter properties for JDBC-based virtual schemas to the user guide and updates the dependencies.

## Documentation

* #71: Updated user guide with reference to common adapter properties for JDBC-based virtual schemas

## Dependency Updates

### Compile Dependency Updates

* Updated `org.postgresql:postgresql:42.5.4` to `42.6.0`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:6.5.1` to `6.6.0`
* Updated `com.exasol:hamcrest-resultset-matcher:1.5.2` to `1.6.0`
* Updated `com.exasol:udf-debugging-java:0.6.8` to `0.6.9`
* Updated `org.junit.jupiter:junit-jupiter:5.9.2` to `5.9.3`
* Updated `org.mockito:mockito-junit-jupiter:5.2.0` to `5.4.0`
* Updated `org.testcontainers:junit-jupiter:1.17.6` to `1.18.3`
* Updated `org.testcontainers:postgresql:1.17.6` to `1.18.3`

### Plugin Dependency Updates

* Updated `com.exasol:error-code-crawler-maven-plugin:1.2.2` to `1.2.3`
* Updated `com.exasol:project-keeper-maven-plugin:2.9.4` to `2.9.7`
* Updated `org.apache.maven.plugins:maven-compiler-plugin:3.10.1` to `3.11.0`
* Updated `org.apache.maven.plugins:maven-enforcer-plugin:3.2.1` to `3.3.0`
* Updated `org.apache.maven.plugins:maven-failsafe-plugin:3.0.0-M8` to `3.0.0`
* Updated `org.apache.maven.plugins:maven-surefire-plugin:3.0.0-M8` to `3.0.0`
* Added `org.basepom.maven:duplicate-finder-maven-plugin:1.5.1`
* Updated `org.codehaus.mojo:flatten-maven-plugin:1.3.0` to `1.4.1`
* Updated `org.codehaus.mojo:versions-maven-plugin:2.14.2` to `2.15.0`
* Updated `org.jacoco:jacoco-maven-plugin:0.8.8` to `0.8.9`
