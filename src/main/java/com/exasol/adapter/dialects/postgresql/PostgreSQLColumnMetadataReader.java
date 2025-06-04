package com.exasol.adapter.dialects.postgresql;

import java.sql.*;
import java.util.logging.Logger;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.dialects.postgresql.PostgreSQLIdentifierMapping.CaseFolding;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.jdbc.JDBCTypeDescription;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements PostgreSQL-specific reading of column metadata.
 */
public class PostgreSQLColumnMetadataReader extends BaseColumnMetadataReader {
    private static final Logger LOGGER = Logger.getLogger(PostgreSQLColumnMetadataReader.class.getName());
    private static final String POSTGRES_VARBIT_TYPE_NAME = "varbit";

    /**
     * Create a new instance of the {@link PostgreSQLColumnMetadataReader}.
     *
     * @param connection          JDBC connection to the remote data source
     * @param properties          user-defined adapter properties
     * @param exaMetadata         metadata of the Exasol database
     * @param identifierConverter converter between source and Exasol identifiers
     */
    public PostgreSQLColumnMetadataReader(final Connection connection, final AdapterProperties properties,
            final ExaMetadata exaMetadata,
            final IdentifierConverter identifierConverter) {
        super(connection, properties, exaMetadata, identifierConverter);
    }

    @Override
    public DataType mapJdbcType(final JDBCTypeDescription jdbcTypeDescription) {
        switch (jdbcTypeDescription.getJdbcType()) {
            case Types.OTHER:
                return mapJdbcTypeOther(jdbcTypeDescription);
            case Types.SQLXML:
            case Types.DISTINCT:
            case Types.BINARY:
                return DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8);
            default:
                return super.mapJdbcType(jdbcTypeDescription);
        }
    }

    protected DataType mapJdbcTypeOther(final JDBCTypeDescription jdbcTypeDescription) {
        if (isVarBitColumn(jdbcTypeDescription)) {
            final int n = jdbcTypeDescription.getPrecisionOrSize();
            LOGGER.finer(() -> "Mapping PostgreSQL datatype \"OTHER:varbit\" to VARCHAR(" + n + ")");
            return DataType.createVarChar(n, DataType.ExaCharset.UTF8);
        } else {
            LOGGER.finer(() -> "Mapping PostgreSQL datatype \"" + jdbcTypeDescription.getTypeName()
                    + "\" to maximum VARCHAR()");
            return DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8);
        }
    }

    protected boolean isVarBitColumn(final JDBCTypeDescription jdbcTypeDescription) {
        return jdbcTypeDescription.getTypeName().equals(POSTGRES_VARBIT_TYPE_NAME);
    }

    @Override
    public String readColumnName(final ResultSet columns) throws SQLException {
        if (getIdentifierMapping().equals(CaseFolding.CONVERT_TO_UPPER)) {
            return super.readColumnName(columns).toUpperCase();
        } else {
            return super.readColumnName(columns);
        }
    }

    CaseFolding getIdentifierMapping() {
        return PostgreSQLIdentifierMapping.from(this.properties);
    }
}
