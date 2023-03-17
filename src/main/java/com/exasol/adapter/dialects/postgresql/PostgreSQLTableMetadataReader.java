package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.AdapterProperties.IGNORE_ERRORS_PROPERTY;

import java.sql.Connection;
import java.util.logging.Logger;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.dialects.postgresql.PostgreSQLIdentifierMapping.CaseFolding;
import com.exasol.adapter.jdbc.*;
import com.exasol.errorreporting.ExaError;

/**
 * This class handles the specifics of mapping PostgreSQL table metadata to Exasol.
 */
public class PostgreSQLTableMetadataReader extends BaseTableMetadataReader {
    static final Logger LOGGER = Logger.getLogger(PostgreSQLTableMetadataReader.class.getName());

    /**
     * Create a new {@link PostgreSQLTableMetadataReader} instance.
     *
     * @param connection           JDBC connection to the remote data source
     * @param columnMetadataReader reader to be used to map the metadata of the tables columns
     * @param properties           user-defined adapter properties
     * @param identifierConverter  converter between source and Exasol identifiers
     */
    public PostgreSQLTableMetadataReader(final Connection connection, final ColumnMetadataReader columnMetadataReader,
            final AdapterProperties properties, final IdentifierConverter identifierConverter) {
        super(connection, columnMetadataReader, properties, identifierConverter);
    }

    /**
     * Get the identifier mapping that the metadata reader uses when mapping PostgreSQL tables to Exasol.
     *
     * @return identifier mapping
     */
    public CaseFolding getIdentifierMapping() {
        return PostgreSQLIdentifierMapping.from(this.properties);
    }

    /**
     * Check if the metadata reader should ignore tables where the name contains upper-case characters.
     *
     * @return <code>true</code> if the reader should ignore upper-case tables
     */
    public boolean ignoresUpperCaseTables() {
        return this.properties.getIgnoredErrors().contains(PostgreSQLIdentifierMapping.UPPERCASE_TABLES_SWITCH);
    }

    @Override
    public boolean isTableIncludedByMapping(final String tableName) {
        if (containsUppercaseCharacter(tableName) && !isUnquotedIdentifier(tableName)) {
            return isUppercaseTableIncludedByMapping(tableName);
        } else {
            return true;
        }
    }

    protected boolean isUppercaseTableIncludedByMapping(final String tableName) {
        if (getIdentifierMapping() == CaseFolding.CONVERT_TO_UPPER) {
            if (ignoresUpperCaseTables()) {
                LOGGER.fine(() -> "Ignoring PostgreSQL table " + tableName
                        + "because it contains an uppercase character and " + IGNORE_ERRORS_PROPERTY + " is set to "
                        + PostgreSQLIdentifierMapping.UPPERCASE_TABLES_SWITCH + ".");
                return false;
            } else {
                throw new RemoteMetadataReaderException(ExaError.messageBuilder("E-VSPG-6")
                        .message("Table {{tableName}} cannot be used in virtual schema.", tableName)
                        .mitigation("Set property {{propertyName}} to {{propertyValue}} to enforce schema creation.",
                                IGNORE_ERRORS_PROPERTY, PostgreSQLIdentifierMapping.UPPERCASE_TABLES_SWITCH)
                        .toString());
            }
        } else {
            return true;
        }
    }

    private boolean containsUppercaseCharacter(final String tableName) {
        for (int i = 0; i < tableName.length(); i++) {
            if (Character.isUpperCase(tableName.charAt(i))) {
                return true;
            }
        }
        return false;
    }
}