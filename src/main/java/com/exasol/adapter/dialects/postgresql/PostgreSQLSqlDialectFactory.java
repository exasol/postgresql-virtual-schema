package com.exasol.adapter.dialects.postgresql;

import com.exasol.adapter.dialects.*;
import com.exasol.logging.VersionCollector;

/**
 * Factory for the PostgreSQL SQL dialect.
 */
public class PostgreSQLSqlDialectFactory implements SqlDialectFactory {

    @Override
    public String getSqlDialectName() {
        return PostgreSQLSqlDialect.NAME;
    }

    @Override
    public SqlDialect createSqlDialect(final JDBCAdapterContext context) {
        return new PostgreSQLSqlDialect(context);
    }

    @Override
    public String getSqlDialectVersion() {
        final VersionCollector versionCollector = new VersionCollector(
                "META-INF/maven/com.exasol/postgresql-virtual-schema/pom.properties");
        return versionCollector.getVersionNumber();
    }

    @Override
    public String getAdapterProjectShortTag() {
        return "VSPG";
    }
}
