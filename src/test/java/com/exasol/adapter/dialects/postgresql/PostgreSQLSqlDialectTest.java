package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.rewriting.ImportIntoTemporaryTableQueryRewriter;
import com.exasol.adapter.jdbc.ConnectionFactory;
import com.exasol.adapter.jdbc.RemoteMetadataReaderException;
import com.exasol.adapter.properties.PropertyValidationException;

@ExtendWith(MockitoExtension.class)
class PostgreSQLSqlDialectTest {
    private PostgreSQLSqlDialect dialect;
    @Mock
    ConnectionFactory connectionFactoryMock;
    @Mock
    ExaMetadata exaMetadataMock;

    @BeforeEach
    void beforeEach() {
        lenient().when(exaMetadataMock.getDatabaseVersion()).thenReturn("8.34.0");
        this.dialect = testee(emptyMap());
    }

    @Test
    void testCreateRemoteMetadataReader() {
        assertThat(this.dialect.createRemoteMetadataReader(), instanceOf(PostgreSQLMetadataReader.class));
    }

    @Test
    void testCreateRemoteMetadataReaderConnectionFails(@Mock final Connection connectionMock) throws SQLException {
        when(this.connectionFactoryMock.getConnection()).thenThrow(new SQLException());
        final RemoteMetadataReaderException exception = assertThrows(RemoteMetadataReaderException.class,
                this.dialect::createRemoteMetadataReader);
        assertThat(exception.getMessage(), containsString("E-VSPG-3"));
    }

    @Test
    void testCreateQueryRewriter() {
        assertThat(this.dialect.createQueryRewriter(), instanceOf(ImportIntoTemporaryTableQueryRewriter.class));
    }

    @Test
    void testGetCapabilities() {
        final Capabilities capabilities = this.dialect.getCapabilities();
        assertAll(
                () -> assertThat(capabilities.getMainCapabilities(),
                        containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                                AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION,
                                AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT,
                                LIMIT_WITH_OFFSET, JOIN, JOIN_TYPE_INNER, JOIN_TYPE_LEFT_OUTER, JOIN_TYPE_RIGHT_OUTER,
                                JOIN_TYPE_FULL_OUTER, JOIN_CONDITION_EQUI)),
                () -> assertThat(capabilities.getLiteralCapabilities(),
                        containsInAnyOrder(BOOL, NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING)),
                () -> assertThat(capabilities.getPredicateCapabilities(),
                        containsInAnyOrder(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN,
                                REGEXP_LIKE, IN_CONSTLIST, IS_NULL, IS_NOT_NULL)),
                () -> assertThat(capabilities.getAggregateFunctionCapabilities(),
                        containsInAnyOrder(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG,
                                AVG_DISTINCT, MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP,
                                STDDEV_POP_DISTINCT, STDDEV_SAMP, STDDEV_SAMP_DISTINCT, VARIANCE, VARIANCE_DISTINCT,
                                VAR_POP, VAR_POP_DISTINCT, VAR_SAMP, VAR_SAMP_DISTINCT, GROUP_CONCAT)) //
        );
    }

    @CsvSource({ "ABC, \"abc\"",
            "AbCde, \"abcde\"",
            "\"tableName, \"\"\"tablename\""
    })
    @ParameterizedTest
    void testApplyQuote(final String unquoted, final String quoted) {
        assertThat(this.dialect.applyQuote(unquoted), equalTo(quoted));
    }

    @ValueSource(strings = { "ab:E'ab'", "a'b:E'a''b'", "a''b:E'a''''b'", "'ab':E'''ab'''", "a\\\\b:E'a\\\\\\\\b'",
            "a\\'b:E'a\\\\''b'" })
    @ParameterizedTest
    void testGetLiteralString(final String definition) {
        assertThat(this.dialect.getStringLiteral(definition.substring(0, definition.indexOf(':'))),
                equalTo(definition.substring(definition.indexOf(':') + 1)));
    }

    @Test
    void testGetLiteralStringNull() {
        assertThat(this.dialect.getStringLiteral(null), CoreMatchers.equalTo("NULL"));
    }

    @Test
    void testPostgreSQLIdentifierMappingConsistency() throws PropertyValidationException {
        final SqlDialect sqlDialect = testee(Map.of(
                CONNECTION_NAME_PROPERTY, "MY_CONN",
                "POSTGRESQL_IDENTIFIER_MAPPING", "CONVERT_TO_UPPER"));
        sqlDialect.validateProperties();
    }

    @Test
    void testPostgreSQLIdentifierMappingInvalidPropertyValueThrowsException() {
        final SqlDialect sqlDialect = testee(Map.of(
                CONNECTION_NAME_PROPERTY, "MY_CONN",
                "POSTGRESQL_IDENTIFIER_MAPPING", "CONVERT"));
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("E-VSPG-4"));
    }

    @Test
    void testIgnoreErrorsConsistency() {
        final SqlDialect sqlDialect = testee(Map.of(
                CONNECTION_NAME_PROPERTY, "MY_CONN",
                "IGNORE_ERRORS", "ORACLE_ERROR"));
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("E-VSPG-5"));
    }

    @Test
    void testValidateCatalogProperty() throws PropertyValidationException {
        final SqlDialect sqlDialect = testee(Map.of(
                CONNECTION_NAME_PROPERTY, "MY_CONN",
                CATALOG_NAME_PROPERTY, "MY_CATALOG"));
        sqlDialect.validateProperties();
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        final SqlDialect sqlDialect = testee(Map.of(
                CONNECTION_NAME_PROPERTY, "MY_CONN",
                SCHEMA_NAME_PROPERTY, "MY_SCHEMA"));
        sqlDialect.validateProperties();
    }

    private PostgreSQLSqlDialect testee(final Map<String, String> properties) {
        return testee(new AdapterProperties(properties));
    }

    private PostgreSQLSqlDialect testee(final AdapterProperties properties) {
        return new PostgreSQLSqlDialect(connectionFactoryMock, properties, exaMetadataMock);
    }
}
