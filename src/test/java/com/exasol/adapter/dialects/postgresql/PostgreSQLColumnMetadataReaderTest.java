package com.exasol.adapter.dialects.postgresql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.postgresql.PostgreSQLIdentifierMapping.CaseFolding;
import com.exasol.adapter.jdbc.JDBCTypeDescription;
import com.exasol.adapter.metadata.DataType;

@ExtendWith(MockitoExtension.class)
class PostgreSQLColumnMetadataReaderTest {
    private PostgreSQLColumnMetadataReader columnMetadataReader;
    private Map<String, String> rawProperties;
    @Mock
    ExaMetadata exaMetadataMock;

    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = createDefaultPostgreSQLColumnMetadataReader();
        this.rawProperties = new HashMap<>();
    }

    private PostgreSQLColumnMetadataReader createDefaultPostgreSQLColumnMetadataReader() {
        when(exaMetadataMock.getDatabaseVersion()).thenReturn("8.34.0");
        return new PostgreSQLColumnMetadataReader(null, AdapterProperties.emptyProperties(), exaMetadataMock,
                BaseIdentifierConverter.createDefault());
    }

    @Test
    void testMapJdbcTypeOther() {
        assertThat(mapJdbcType(Types.OTHER), equalTo(DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8)));
    }

    protected DataType mapJdbcType(final int type) {
        final JDBCTypeDescription jdbcTypeDescription = new JDBCTypeDescription(type, 0, 0, 0, "");
        return this.columnMetadataReader.mapJdbcType(jdbcTypeDescription);
    }

    @ValueSource(ints = { Types.SQLXML, Types.DISTINCT })
    @ParameterizedTest
    void testMapJdbcTypeFallbackToMaxVarChar(final int type) {
        assertThat(mapJdbcType(type), equalTo(DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8)));
    }

    @Test
    void testMapJdbcTypeFallbackToParent() {
        assertThat(mapJdbcType(Types.BOOLEAN), equalTo(DataType.createBool()));
    }

    @Test
    void testGetDefaultPostgreSQLIdentifierMapping() {
        assertThat(this.columnMetadataReader.getIdentifierMapping(), equalTo(CaseFolding.CONVERT_TO_UPPER));
    }

    @Test
    void testGetPreserveCasePostgreSQLIdentifierMapping() {
        this.rawProperties.put("POSTGRESQL_IDENTIFIER_MAPPING", "PRESERVE_ORIGINAL_CASE");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final PostgreSQLColumnMetadataReader testee = new PostgreSQLColumnMetadataReader(null,
                adapterProperties, exaMetadataMock, BaseIdentifierConverter.createDefault());
        assertThat(testee.getIdentifierMapping(), equalTo(CaseFolding.PRESERVE_ORIGINAL_CASE));
    }

    @Test
    void testGetConverToUpperPostgreSQLIdentifierMapping() {
        this.rawProperties.put("POSTGRESQL_IDENTIFIER_MAPPING", "CONVERT_TO_UPPER");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final PostgreSQLColumnMetadataReader testee = new PostgreSQLColumnMetadataReader(null,
                adapterProperties, exaMetadataMock, BaseIdentifierConverter.createDefault());
        assertThat(testee.getIdentifierMapping(), equalTo(CaseFolding.CONVERT_TO_UPPER));
    }
}
