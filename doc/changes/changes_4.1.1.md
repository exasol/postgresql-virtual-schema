# Virtual Schema for PostgreSQL 4.1.1, released 2026-??-??

Code name: Stabilize timezone-aware datatype tests

## Summary

This release stabilizes integration tests for PostgreSQL timezone-aware datatypes across daylight saving time changes.

## Bugfixes

* #92: Stabilized integration tests for PostgreSQL `TIME WITH TIME ZONE` and `TIMESTAMP WITH TIME ZONE` values.

## Dependency Updates

### Plugin Dependency Updates

* Updated `com.exasol:project-keeper-maven-plugin:5.7.7` to `5.8.0`
