package com.exasol.adapter.dialects.postgresql;

import java.util.regex.Pattern;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierCaseHandling;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.dialects.postgresql.PostgreSQLIdentifierMapping.CaseFolding;

/**
 * This class implements database identifier converter for {@link PostgreSQLSqlDialect}.
 */
public class PostgreSQLIdentifierConverter implements IdentifierConverter {
    private static final Pattern UNQUOTED_IDENTIFIER_PATTERN = Pattern.compile("^[a-z][0-9a-z_]*");
    private final AdapterProperties properties;

    /**
     * Create a new instance of the {@link PostgreSQLIdentifierConverter}.
     *
     * @param properties adapter properties
     */
    public PostgreSQLIdentifierConverter(final AdapterProperties properties) {
        this.properties = properties;
    }

    @Override
    public String convert(final String identifier) {
        if (getIdentifierMapping() == CaseFolding.PRESERVE_ORIGINAL_CASE) {
            return identifier;
        } else {
            if (isUnquotedIdentifier(identifier)) {
                return identifier.toUpperCase();
            } else {
                return identifier;
            }
        }
    }

    /**
     * Get the identifier mapping that the metadata reader uses when mapping PostgreSQL tables to Exasol.
     *
     * @return identifier mapping
     */
    public CaseFolding getIdentifierMapping() {
        return PostgreSQLIdentifierMapping.from(this.properties);
    }

    private boolean isUnquotedIdentifier(final String identifier) {
        return UNQUOTED_IDENTIFIER_PATTERN.matcher(identifier).matches();
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_LOWER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
    }
}