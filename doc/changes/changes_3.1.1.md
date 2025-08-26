# Virtual Schema for PostgreSQL 3.1.1, released 2025-08-26

Code name: Fixes for vulnerabilities CVE-2025-48924 and CVE-2025-49146

## Summary

This release fixes the following vulnerabilities:

### CVE-2025-48924 (CWE-674) in dependency `org.apache.commons:commons-lang3:jar:3.16.0:test`

Uncontrolled Recursion vulnerability in Apache Commons Lang.

This issue affects Apache Commons Lang: Starting with commons-lang:commons-lang 2.0 to 2.6, and, from org.apache.commons:commons-lang3 3.0 before 3.18.0.

The methods ClassUtils.getClass(...) can throw StackOverflowError on very long inputs. Because an Error is usually not handled by applications and libraries, a 
StackOverflowError could cause an application to stop.

Users are recommended to upgrade to version 3.18.0, which fixes the issue.

CVE: CVE-2025-48924
CWE: CWE-674

#### References

- https://ossindex.sonatype.org/vulnerability/CVE-2025-48924?component-type=maven&component-name=org.apache.commons%2Fcommons-lang3&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
- http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2025-48924
- https://github.com/advisories/GHSA-j288-q9x7-2f5v

### CVE-2025-49146 (CWE-287) in dependency `org.postgresql:postgresql:jar:42.7.6:compile`

postgresql - Improper Authentication

CVE: CVE-2025-49146
CWE: CWE-287

#### References

- https://ossindex.sonatype.org/vulnerability/CVE-2025-49146?component-type=maven&component-name=org.postgresql%2Fpostgresql&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
- http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2025-49146
- https://github.com/advisories/GHSA-hq9p-pm7w-8p54
- https://gitlab.com/gitlab-org/advisories-community/-/blob/main/maven/org.postgresql/postgresql/CVE-2025-49146.yml
- https://nvd.nist.gov/vuln/detail/CVE-2025-49146
- https://osv-vulnerabilities.storage.googleapis.com/Maven/GHSA-hq9p-pm7w-8p54.json

## Security

* #86: Fixed vulnerability CVE-2025-48924 in dependency `org.apache.commons:commons-lang3:jar:3.16.0:test`
* #85: Fixed vulnerability CVE-2025-49146 in dependency `org.postgresql:postgresql:jar:42.7.6:compile`

## Dependency Updates

### Compile Dependency Updates

* Updated `org.postgresql:postgresql:42.7.6` to `42.7.7`

### Test Dependency Updates

* Updated `com.exasol:exasol-testcontainers:7.1.5` to `7.1.7`

### Plugin Dependency Updates

* Updated `com.exasol:error-code-crawler-maven-plugin:2.0.3` to `2.0.4`
* Updated `com.exasol:project-keeper-maven-plugin:5.1.0` to `5.2.3`
