package com.exasol.adapter.dialects.postgresql;

import com.exasol.errorreporting.ExaError;

/**
 * This enumeration defines the behavior of PostgresSQL when it comes to dealing with unquoted identifiers (e.g. table
 * names).
 */
public enum PostgreSQLIdentifierMapping {
    CONVERT_TO_UPPER, PRESERVE_ORIGINAL_CASE;

    /**
     * Parse the identifier mapping from a string.
     *
     * @param mapping string describing the mapping
     * @return PosgreSQL identifier mapping
     * @throws IllegalArgumentException if the given string contains a mapping name that is unknown or
     *                                  <code>null</code>.
     */
    public static PostgreSQLIdentifierMapping parse(final String mapping) {
        if (mapping != null) {
            switch (mapping) {
            case "CONVERT_TO_UPPER":
                return CONVERT_TO_UPPER;
            case "PRESERVE_ORIGINAL_CASE":
                return PRESERVE_ORIGINAL_CASE;
            default:
                throw new IllegalArgumentException(ExaError.messageBuilder("E-PGVS-2")
                        .message("Unable to parse PostgreSQL identifier mapping {{mapping}}.", mapping).toString());
            }
        } else {
            throw new IllegalArgumentException(ExaError.messageBuilder("E-PGVS-1")
                    .message("Unable to parse PostgreSQL identifier mapping from a null value.").toString());
        }
    }
}
