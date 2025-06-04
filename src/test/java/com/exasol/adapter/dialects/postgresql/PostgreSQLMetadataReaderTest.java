package com.exasol.adapter.dialects.postgresql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierCaseHandling;
import com.exasol.adapter.dialects.IdentifierConverter;

@ExtendWith(MockitoExtension.class)
class PostgreSQLMetadataReaderTest {
    private PostgreSQLMetadataReader reader;
    @Mock
    ExaMetadata exaMetadataMock;

    @BeforeEach
    void beforeEach() {
        when(exaMetadataMock.getDatabaseVersion()).thenReturn("8.34.0");
        this.reader = new PostgreSQLMetadataReader(null, AdapterProperties.emptyProperties(), exaMetadataMock);
    }

    @Test
    void testGetTableMetadataReader() {
        assertThat(this.reader.getTableMetadataReader(), instanceOf(PostgreSQLTableMetadataReader.class));
    }

    @Test
    void testGetColumnMetadataReader() {
        assertThat(this.reader.getColumnMetadataReader(), instanceOf(PostgreSQLColumnMetadataReader.class));
    }

    @Test
    void testGetIdentifierConverter() {
        final IdentifierConverter identifierConverter = this.reader.getIdentifierConverter();
        assertAll(() -> assertThat(identifierConverter, instanceOf(PostgreSQLIdentifierConverter.class)),
                () -> assertThat(identifierConverter.getQuotedIdentifierHandling(),
                        equalTo(IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE)),
                () -> assertThat(identifierConverter.getUnquotedIdentifierHandling(),
                        equalTo(IdentifierCaseHandling.INTERPRET_AS_LOWER)));
    }
}
