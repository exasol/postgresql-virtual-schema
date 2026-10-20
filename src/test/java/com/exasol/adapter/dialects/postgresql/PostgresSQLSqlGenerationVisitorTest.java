package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToOne;
import static com.exasol.adapter.sql.ScalarFunction.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.dialects.rewriting.SqlGenerationContext;
import com.exasol.adapter.jdbc.ConnectionFactory;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.*;

@ExtendWith(MockitoExtension.class)
class PostgresSQLSqlGenerationVisitorTest {
    private PostgresSQLSqlGenerationVisitor visitor;

    @BeforeEach
    void beforeEach(@Mock final ConnectionFactory connectionFactoryMock) {
        final SqlDialect dialect = new PostgreSQLSqlDialect(JDBCAdapterContext.builder()
                .connectionFactory(connectionFactoryMock)
                .properties(AdapterProperties.emptyProperties())
                .build());
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new PostgresSQLSqlGenerationVisitor(dialect, context);
    }

    @CsvSource({ "ADD_DAYS, days",
            "ADD_HOURS, hours",
            "ADD_MINUTES, mins",
            "ADD_SECONDS, secs",
            "ADD_YEARS, years",
            "ADD_WEEKS, weeks",
            "ADD_MONTHS, months" })
    @ParameterizedTest
    void testVisitSqlFunctionScalarAddDate(final ScalarFunction scalarFunction, final String expected)
            throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(scalarFunction, 10);
        assertThat(this.visitor.visit(sqlFunctionScalar),
                equalTo("\"test_column\" + make_interval(" + expected + " => 10)"));
    }

    private SqlFunctionScalar createSqlFunctionScalarForDateTest(final ScalarFunction scalarFunction,
            final int numericValue) {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column")
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}")
                        .type(DataType.createChar(20, DataType.ExaCharset.UTF8)).build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(numericValue)));
        return new SqlFunctionScalar(scalarFunction, arguments);
    }

    @CsvSource({ "SECOND, SECOND, 2",
            "MINUTE, MINUTE, 2",
            "DAY, DAY, 2",
            "WEEK, WEEK, 2",
            "MONTH, MONTH, 2",
            "YEAR, YEAR, 4" })
    @ParameterizedTest
    void testVisitSqlFunctionScalarDatetime(final ScalarFunction scalarFunction, final String expected,
            final String decimalSize) throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(scalarFunction, 0);
        assertThat(this.visitor.visit(sqlFunctionScalar),
                equalTo("CAST(DATE_PART('" + expected + "',\"test_column\") AS DECIMAL(" + decimalSize + ",0))"));
    }

    @Test
    void testVisitSqlFunctionScalarPosixTime() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(POSIX_TIME, 0);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("EXTRACT(EPOCH FROM \"test_column\")"));
    }

    @Test
    void testVisitSqlFunctionScalarDayOfWeek() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(DAYOFWEEK, 0);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("(EXTRACT(DOW FROM \"test_column\") + 1)"));
    }

    @Test
    void testVisitSqlFunctionScalarLastDay() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(LAST_DAY, 0);
        assertThat(this.visitor.visit(sqlFunctionScalar),
                equalTo("CAST((DATE_TRUNC('month', \"test_column\") + INTERVAL '1 month - 1 day') AS DATE)"));
    }

    @Test
    void testVisitSqlFunctionScalarFloatDivision() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(FLOAT_DIV, 10);
        assertThat(this.visitor.visit(sqlFunctionScalar),
                equalTo("( CAST (\"test_column\" AS DOUBLE PRECISION) / CAST (10 AS DOUBLE PRECISION))"));
    }

    @Test
    void testVisitSqlFunctionScalarDefault() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ABS,
                List.of(new SqlColumn(1, ColumnMetadata.builder().name("test_column")
                        .type(DataType.createDouble()).build())));
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("ABS(\"test_column\")"));
    }

    @Test
    void testAppendDatePart() {
        final StringBuilder builder = new StringBuilder();
        PostgresSQLSqlGenerationVisitor.appendDatePart(ADD_DAYS, builder);
        assertThat(builder.toString(), equalTo(""));
    }

    @Test
    void testAppendDecimalSizeDefaults() {
        final StringBuilder builder = new StringBuilder();
        PostgresSQLSqlGenerationVisitor.appendDecimalSize(ADD_DAYS, builder);
        assertThat(builder.toString(), equalTo(""));
    }

    @Test
    void testVisitSqlSelectListAnyValue() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createAnyValueSelectList();
        assertSqlNodeConvertedToOne(sqlSelectList, this.visitor);
    }

    @CsvSource({ "point, CAST(\"test_column\"  as VARCHAR )",
            "smallserial, CAST(\"test_column\"  as SMALLINT )",
            "serial, CAST(\"test_column\"  as INTEGER )",
            "bigserial, CAST(\"test_column\"  as BIGINT )",
            "bytea, cast('bytea NOT SUPPORTED' as varchar) as not_supported",
            "text, \"test_column\"" })
    @ParameterizedTest
    void testVisitSqlColumnProjection(final String typeName, final String expected) throws AdapterException {
        final SqlColumn column = new SqlColumn(1, ColumnMetadata.builder().name("test_column")
                .adapterNotes("{\"jdbcDataType\":1111,\"typeName\":\"" + typeName + "\"}")
                .type(DataType.createChar(20, DataType.ExaCharset.UTF8)).build());
        SqlSelectList.createRegularSelectList(List.of(column));

        assertThat(this.visitor.visit(column), equalTo(expected));
    }

    @Test
    void testVisitSqlStatementSelect() throws AdapterException {
        final SqlStatementSelect select = (SqlStatementSelect) DialectTestData.getTestSqlNode();
        assertThat(this.visitor.visit(select),
                equalTo("SELECT \"user_id\", "
                        + "COUNT(\"url\") FROM \"test_schema\".\"clicks\" "
                        + "WHERE 1 < \"user_id\" "
                        + "GROUP BY \"user_id\" "
                        + "HAVING 1 < COUNT(\"url\") "
                        + "ORDER BY \"user_id\" LIMIT 10"));
    }

    @CsvSource(value = { "true;STRING_AGG(E'test', E'''')", "false;STRING_AGG(E'test', ',')" }, delimiter = ';')
    @ParameterizedTest
    void testVisitSqlFunctionAggregateGroupConcat(final boolean withSeparator, final String expected)
            throws AdapterException {
        final SqlLiteralString argument = new SqlLiteralString("test");
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .build();
        final ColumnMetadata columnMetadata2 = ColumnMetadata.builder().name("test_column2")
                .type(DataType.createDouble()).build();
        final List<SqlNode> orderByArguments = List.of(new SqlColumn(1, columnMetadata),
                new SqlColumn(2, columnMetadata2));
        final SqlOrderBy orderBy = new SqlOrderBy(orderByArguments, List.of(false, true), List.of(false, true));
        final SqlFunctionAggregateGroupConcat.Builder builder = SqlFunctionAggregateGroupConcat.builder(argument)
                .orderBy(orderBy);
        if (withSeparator) {
            builder.separator(new SqlLiteralString("'"));
        }
        assertThat(this.visitor.visit(builder.build()), equalTo(expected + " "));
    }
}
