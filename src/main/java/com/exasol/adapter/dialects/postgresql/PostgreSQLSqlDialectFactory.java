package com.exasol.adapter.dialects.postgresql;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;
import com.exasol.adapter.jdbc.ConnectionFactory;
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
    public SqlDialect createSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties,
            final ExaMetadata exaMetadata) {
        return new PostgreSQLSqlDialect(connectionFactory, properties, exaMetadata);
    }

    @Override
    public String getSqlDialectVersion() {
        final VersionCollector versionCollector = new VersionCollector(
                "META-INF/maven/com.exasol/virtual-schema-jdbc-adapter/pom.properties");
        return versionCollector.getVersionNumber();
    }
}
