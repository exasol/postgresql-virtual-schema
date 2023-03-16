package com.exasol.adapter.dialects.postgresql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.dialects.postgresql.PostgreSQLIdentifierMapping.CaseFolding;

class PostgreSQLIdentifierMappingTest {
    @Test
    void testParseConvertToUpperCase() {
        assertThat(PostgreSQLIdentifierMapping.parse("CONVERT_TO_UPPER"), equalTo(CaseFolding.CONVERT_TO_UPPER));
    }

    @Test
    void testParseConvertToPreserverOriginalCase() {
        assertThat(PostgreSQLIdentifierMapping.parse("PRESERVE_ORIGINAL_CASE"),
                equalTo(CaseFolding.PRESERVE_ORIGINAL_CASE));
    }

    @Test
    void testParseNullMappingThrowsException() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> PostgreSQLIdentifierMapping.parse(null));
        assertThat(exception.getMessage(), containsString("E-VSPG-1"));
    }

    @Test
    void testParseUnknownMappingThrowsException() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> PostgreSQLIdentifierMapping.parse("UNKNOWN"));
        assertThat(exception.getMessage(), containsString("E-VSPG-2"));
    }
}