# Virtual Schema for PostgreSQL 4.0.2, released 2026-08-04

Code name: Support legacy nullable metadata

## Summary

This release supports legacy nullable metadata.

## Bugfixes

* #96: Handle `null` in `isNullable` metadata after DB upgrade to 2025.1.12

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:virtual-schema-common-jdbc:14.0.4` to `14.0.5`

### Test Dependency Updates

* Updated `com.exasol:virtual-schema-common-jdbc:14.0.4` to `14.0.5`
* Updated `com.exasol:virtual-schema-shared-integration-tests:3.0.2` to `3.0.3`
