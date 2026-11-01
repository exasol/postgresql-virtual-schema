# Virtual Schema for PostgreSQL 4.1.1, released 2026-??-??

Code name: Stabilize timezone-aware datatype tests, fixed vulnerability CVE-2026-116486 in org.objenesis:objenesis:jar:3.3:test

## Summary

This release stabilizes integration tests for PostgreSQL timezone-aware datatypes across daylight saving time changes.

This release fixes the following vulnerability:

### CVE-2026-116486 (CWE-386) in dependency `org.objenesis:objenesis:jar:3.3:test`
Objenesis before 3.6 contains a type confusion vulnerability that allows code in one class loader to substitute objects into another by exploiting instantiator caching keyed on class name. When a shared ObjenesisStd instance caches instantiators by clazz.getName() in ObjenesisBase.getInstantiatorOf(), same-named classes collide, causing newInstance() to return wrong-class objects and ClassCastException failures.
#### References
* https://guide.sonatype.com/vulnerability/CVE-2026-116486?component-type=maven&component-name=org.objenesis%2Fobjenesis&utm_source=ossindex-client&utm_medium=integration&utm_content=1.8.1
* http://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2026-116486
* https://github.com/CVEProject/cvelistV5/blob/main/cves/2026/116xxx/CVE-2026-116486.json

## Security

* #118: Fixed vulnerability CVE-2026-116486 in dependency `org.objenesis:objenesis:jar:3.3:test`

## Bugfixes

* #92: Stabilized integration tests for PostgreSQL `TIME WITH TIME ZONE` and `TIMESTAMP WITH TIME ZONE` values.

## Dependency Updates

### Compile Dependency Updates

* Updated `com.exasol:virtual-schema-common-jdbc:14.1.0` to `14.1.1`

### Test Dependency Updates

* Updated `com.exasol:virtual-schema-common-jdbc:14.1.0` to `14.1.1`
* Updated `org.junit.jupiter:junit-jupiter:5.14.4` to `6.1.3`

### Plugin Dependency Updates

* Updated `com.exasol:project-keeper-maven-plugin:5.7.7` to `5.8.2`
