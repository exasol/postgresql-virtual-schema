# Virtual Schema for PostgreSQL 2.0.3, released 2022-06-24

Code name: Dependency Updates

## Summary

In this release we updated dependencies and by that fixed the following security vulnerabilities:

* CVE-2022-24823
* sonatype-2020-0026
* CVE-2016-5003
* CVE-2016-5002
* CVE-2021-22569
* CVE-2016-5004

## Dependency Updates

### Compile Dependency Updates

* Updated `org.postgresql:postgresql:42.3.3` to `42.4.0`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:6.1.1` to `6.1.2`
* Updated `com.exasol:test-db-builder-java:3.3.1` to `3.3.3`
* Updated `com.exasol:udf-debugging-java:0.6.0` to `0.6.3`
* Updated `org.mockito:mockito-junit-jupiter:4.3.1` to `4.6.1`
* Updated `org.testcontainers:junit-jupiter:1.16.3` to `1.17.2`
* Updated `org.testcontainers:postgresql:1.16.3` to `1.17.2`

### Plugin Dependency Updates

* Updated `com.exasol:artifact-reference-checker-maven-plugin:0.4.1` to `0.4.0`
* Updated `com.exasol:error-code-crawler-maven-plugin:1.0.0` to `1.1.1`
* Updated `com.exasol:project-keeper-maven-plugin:2.0.0` to `2.4.6`
* Updated `org.apache.maven.plugins:maven-clean-plugin:3.1.0` to `2.5`
* Updated `org.apache.maven.plugins:maven-compiler-plugin:3.10.0` to `3.10.1`
* Updated `org.apache.maven.plugins:maven-dependency-plugin:3.2.0` to `2.8`
* Updated `org.apache.maven.plugins:maven-deploy-plugin:2.8.2` to `2.7`
* Updated `org.apache.maven.plugins:maven-install-plugin:2.5.2` to `2.4`
* Updated `org.apache.maven.plugins:maven-resources-plugin:3.2.0` to `2.6`
* Updated `org.apache.maven.plugins:maven-site-plugin:3.11.0` to `3.3`
* Updated `org.codehaus.mojo:versions-maven-plugin:2.9.0` to `2.10.0`
* Updated `org.jacoco:jacoco-maven-plugin:0.8.7` to `0.8.8`
* Added `org.sonarsource.scanner.maven:sonar-maven-plugin:3.9.1.2184`
