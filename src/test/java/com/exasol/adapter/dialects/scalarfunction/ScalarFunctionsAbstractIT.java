package com.exasol.adapter.dialects.scalarfunction;

import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.errorreporting.ExaError;
import com.exasol.matcher.TypeMatchMode;

/**
 * This is an abstract smoke test for all scalar functions, except the geospacial functions (ST_*).
 */
@Testcontainers
@SuppressWarnings("java:S5786") // class is public since it is an abstract test
@Execution(value = ExecutionMode.SAME_THREAD)
public abstract class ScalarFunctionsAbstractIT {
    /**
     * These have a special syntax, so we define explicit test for them below.
     */
    private static final Set<ScalarFunctionCapability> EXCLUDES = Set.of(CASE, CAST, FLOAT_DIV, SESSION_PARAMETER, RAND,
            ADD, SUB, MULT, NEG, SYS_GUID, SYSTIMESTAMP, CONVERT_TZ, DATE_TRUNC, NUMTODSINTERVAL, NUMTOYMINTERVAL,
            TO_YMINTERVAL, TO_DSINTERVAL, JSON_VALUE, EXTRACT, POSIX_TIME);
    private static final String LOCAL_COPY_TABLE_NAME = "LOCAL_COPY";
    private static final String LOCAL_COPY_SCHEMA = "EXASOL";

    /**
     * These functions are tested separately in
     * {@link WithNoParameters#testFunctionsWithNoParenthesis(ScalarFunctionCapability)}
     */
    private static final Set<ScalarFunctionCapability> FUNCTIONS_WITH_NO_PARENTHESIS = Set.of(LOCALTIMESTAMP, SYSDATE,
            CURRENT_SCHEMA, CURRENT_STATEMENT, CURRENT_SESSION, CURRENT_DATE, CURRENT_USER, CURRENT_TIMESTAMP);
    private static final String LOCAL_COPY_FULL_TABLE_NAME = LOCAL_COPY_SCHEMA + "." + LOCAL_COPY_TABLE_NAME;

    /**
     * For sum functions we just can guess the parameters because they only work for very specific input. For those we
     * define explicit parameters here. You can define multiple parameter combinations here (by adding multiple list
     * entries). By that you can test a function with different values.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ScalarFunctionsAbstractIT.class);
    private Set<ScalarFunctionCapability> dialectSpecificExcludes;

    /**
     * Returns a set of scalar functions that should not be tested for this dialect.
     * 
     * @return set of functions
     */
    protected abstract Set<ScalarFunctionCapability> getDialectSpecificExcludes();

    /**
     * Prepare a Virtual Schema with one table that contains columns for the following types: floating point, integer,
     * string, date, timestamp
     * <p>
     * The table must contain exactly one row with examples for each type. Don't use null or 0 as example values.
     * </p>
     *
     * @return the fully qualified name of the table in the virtual schema.
     */
    protected abstract String createVirtualSchemaTableWithExamplesForAllDataTypes() throws SQLException;

    protected abstract Connection createExasolConnection() throws SQLException;

    protected abstract String createAnyVirtualSchemaTable() throws SQLException;

    protected abstract String createDateVirtualSchemaTableWithSoleValue(Timestamp soleValue) throws SQLException;

    /**
     * Create a table with only one integer column, and only one row with given value.
     * 
     * @param soleRowValue value.
     * @return the fully qualified name of the table in the virtual schema.
     */
    protected abstract String createIntegerVirtualSchemaTableWithSoleRowValue(int soleRowValue) throws SQLException;

    protected abstract String createBooleanVirtualSchemaTableWithSoleTrueValue() throws SQLException;

    protected abstract StringTestTable createStringVirtualSchemaTable() throws SQLException;

    protected abstract void deleteCreatedTables();

    private void runOnExasol(final ExasolExecutable exasolExecutable) {
        try (final Connection connection = createExasolConnection();
                final Statement statement = connection.createStatement()) {
            exasolExecutable.runOnExasol(statement);
        } catch (final SQLException exception) {
            throw new IllegalStateException(
                    ExaError.messageBuilder("E-PGVS-12").message("Failed to execute command on Exasol.").toString(),
                    exception);
        }
    }

    private List<String> getColumnsOfTable(final String tableName) throws SQLException {
        final List<String> names = new ArrayList<>();
        runOnExasol(statement -> {
            try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM " + tableName)) {
                final ResultSetMetaData metaData = resultSet.getMetaData();
                final int columnCount = metaData.getColumnCount();
                for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                    names.add("\"" + metaData.getColumnName(columnIndex) + "\"");
                }
            }
        });
        return names;
    }

    @FunctionalInterface
    private interface ExasolExecutable {
        void runOnExasol(Statement statement) throws SQLException;
    }

    /**
     * This is a workaround to disable tests. The proper method would be to add these tests to the failsafe plugins
     * excludes. This is however not possible due to a bug: https://issues.apache.org/jira/browse/SUREFIRE-1880
     * 
     * @param function function to test
     * @return {@code true} if test is disabled by dialect
     */
    private boolean isTestDisabled(final ScalarFunctionCapability function) {
        this.dialectSpecificExcludes = getDialectSpecificExcludes();
        if (this.dialectSpecificExcludes.contains(function)) {
            LOGGER.info("Skipping test for {} since it was disabled for this dialect.", function.name());
            return true;
        }
        return false;
    }

    /**
     * This test automatically finds parameter combinations by permuting a set of different values. Then it verifies
     * that on of the parameter combinations that did not cause an exception in on a regular Exasol table, succeeds on
     * the virtual schema tale. In addition this test asserts that all queries that succeed on the virtual and the
     * regular table have the same result.
     */
    @Nested
    @TestInstance(PER_CLASS)
    @Tag("WithAutomaticParameterDiscovery")
    class WithAutomaticParameterDiscovery {
        private String fullyQualifiedNameOfVsTable;
        private List<String> columnsWithType;
        private ScalarFunctionsParameterCache parameterCache;
        private ScalarFunctionParameterFinder parameterFinder;
        private VirtualSchemaRunVerifier virtualSchemaRunVerifier;

        @BeforeAll
        void beforeAll() throws SQLException {
            this.fullyQualifiedNameOfVsTable = createVirtualSchemaTableWithExamplesForAllDataTypes();
            runOnExasol(statement -> {
                statement.executeUpdate("CREATE SCHEMA " + LOCAL_COPY_SCHEMA);
                statement.executeUpdate("CREATE TABLE " + LOCAL_COPY_FULL_TABLE_NAME + " as SELECT * FROM "
                        + this.fullyQualifiedNameOfVsTable);
                this.columnsWithType = getColumnsOfTable(LOCAL_COPY_FULL_TABLE_NAME);
                this.parameterCache = new ScalarFunctionsParameterCache();
                this.parameterFinder = new ScalarFunctionParameterFinder(this.columnsWithType,
                        new ScalarFunctionQueryBuilder(LOCAL_COPY_FULL_TABLE_NAME), this.parameterCache);
                this.virtualSchemaRunVerifier = new VirtualSchemaRunVerifier(
                        new ScalarFunctionQueryBuilder(this.fullyQualifiedNameOfVsTable));
            });
        }

        @AfterAll
        void afterAll() {
            deleteCreatedTables();
        }

        /**
         * Test for most of the scala functions. Since they are so many, it's too much effort to write all parameter
         * combinations here. Instead this test tries all permutations on an Exasol table and in case they do not cause
         * an exception asserts that they produce the same result on the virtual schema table.
         *
         * @param function function to test
         */
        @ParameterizedTest
        @MethodSource("getScalarFunctions")
        void testScalarFunctions(final ScalarFunctionCapability function) {
            runOnExasol(statement -> {
                final List<ExasolRun> successfulExasolRuns = this.parameterFinder.findOrGetFittingParameters(function,
                        statement);
                if (successfulExasolRuns.isEmpty()) {
                    throw new IllegalStateException(ExaError.messageBuilder("E-PGVS-14")
                            .message("Non of the parameter combinations lead to a successful run.").toString());
                } else {
                    this.parameterCache.removeFunction(function.name());
                    if (!this.virtualSchemaRunVerifier.quickCheckIfFunctionBehavesSameOnVs(function,
                            successfulExasolRuns, statement)) {
                        LOGGER.info("Quick test failed for {}. Running full checks.", function);
                        final List<String> successfulParameters = this.virtualSchemaRunVerifier
                                .assertFunctionBehavesSameOnVirtualSchema(function, successfulExasolRuns, statement);
                        this.parameterCache.setFunctionsValidParameterCombinations(function.name(),
                                successfulParameters);
                    } else {
                        this.parameterCache.setFunctionsValidParameterCombinations(function.name(), successfulExasolRuns
                                .stream().map(ExasolRun::getParameters).collect(Collectors.toList()));
                    }
                    this.parameterCache.flush();
                }
            });
        }

        Stream<Arguments> getScalarFunctions() {
            return Arrays.stream(ScalarFunctionCapability.values())//
                    .filter(function -> !EXCLUDES.contains(function) && !isTestDisabled(function)
                            && !FUNCTIONS_WITH_NO_PARENTHESIS.contains(function) && !function.name().startsWith("ST_"))//
                    .map(Arguments::of);
        }
    }

    @Nested
    @TestInstance(PER_CLASS)
    @Tag("WithNoParameters") // workaround since IDEA can find the test otherwise
    class WithNoParameters {
        private ScalarFunctionQueryBuilder queryBuilder;

        private Stream<Arguments> getScalarFunctionWithNoParameters() {
            return FUNCTIONS_WITH_NO_PARENTHESIS.stream().filter(function -> !isTestDisabled(function))
                    .map(Arguments::of);
        }

        @BeforeAll
        void beforeAll() throws SQLException {
            this.queryBuilder = new ScalarFunctionQueryBuilder(createAnyVirtualSchemaTable());
        }

        @AfterAll
        void afterAll() {
            deleteCreatedTables();
        }

        /**
         * This test tests functions that do not have parenthesis (e.g. CURRENT_SCHEMA).
         * <p>
         * Since the result of all of this functions is different on different databases or at different time, we just
         * test that they don't throw an exception on the Virtual Schema.
         * </p>
         *
         * @param function function to test.
         */
        @ParameterizedTest
        @MethodSource("getScalarFunctionWithNoParameters")
        void testFunctionsWithNoParenthesis(final ScalarFunctionCapability function) throws SQLException {
            runOnExasol(statement -> {
                assertDoesNotThrow(
                        () -> statement.executeQuery(this.queryBuilder.buildQueryFor(function.name())).close());
            });
        }

        @Test
        void testSystimestamp() {
            if (isTestDisabled(SYSTIMESTAMP))
                return;
            runOnExasol(statement -> {
                statement.executeUpdate("ALTER SESSION SET TIME_ZONE='UTC';");
                try (final ResultSet result = statement
                        .executeQuery(this.queryBuilder.buildQueryFor(("SYSTIMESTAMP")))) {
                    result.next();
                    final LocalDateTime timestamp = result.getTimestamp(1).toLocalDateTime();
                    assertAll(() -> assertTrue(timestamp.isAfter(LocalDateTime.now().minusMinutes(20))),
                            () -> assertTrue(timestamp.isBefore(LocalDateTime.now().plusMinutes(1))));
                }
            });
        }

        /**
         * For the random function it obviously makes no sense to check if the output is the same on two runs. For that
         * reason we check here if it produces at least some different results.
         */
        @Test
        void testRand() {
            if (isTestDisabled(RAND))
                return;
            runOnExasol(statement -> {
                final String query = this.queryBuilder.buildQueryFor("RAND()");
                try (final ResultSet result = statement.executeQuery(query)) {
                    result.next();
                    final double value = result.getDouble(1);
                    assertThat(Double.isFinite(value), equalTo(true));
                }
            });
        }

        @Test
        void testSysGUID() {
            if (isTestDisabled(SYS_GUID))
                return;
            runOnExasol(statement -> {
                assertDoesNotThrow(() -> statement.executeQuery(this.queryBuilder.buildQueryFor("SYS_GUID()")).close());
            });
        }
    }

    @Nested
    @TestInstance(PER_CLASS)
    @Tag("WithIntegerParameter") // workaround since IDEA can find the test otherwise
    class WithIntegerParameter {

        private ScalarFunctionQueryBuilder queryBuilder;
        private String columnName;

        @BeforeAll
        void beforeAll() throws SQLException {
            final String tableName = createIntegerVirtualSchemaTableWithSoleRowValue(2);
            this.queryBuilder = new ScalarFunctionQueryBuilder(tableName);
            this.columnName = getColumnsOfTable(tableName).get(0);
        }

        @AfterAll
        void afterAll() {
            deleteCreatedTables();
        }

        @ParameterizedTest
        @CsvSource({ "ADD, +, 4", "SUB, -, 0", "MULT, *, 4", "FLOAT_DIV, /, 1" })
        void testSimpleArithmeticFunctions(final ScalarFunctionCapability function, final String operator,
                final int expectedResult) {
            if (isTestDisabled(function))
                return;
            final String query = this.columnName + " " + operator + " " + this.columnName;
            runOnExasol(statement -> {
                try (final ResultSet virtualSchemaTableResult = statement
                        .executeQuery(this.queryBuilder.buildQueryFor(query))) {
                    assertThat(virtualSchemaTableResult,
                            table().row(expectedResult).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
                }
            });
        }

        @Test
        void testCast() {
            if (isTestDisabled(CAST))
                return;
            runOnExasol(statement -> {
                final String virtualSchemaQuery = this.queryBuilder
                        .buildQueryFor("CAST(" + this.columnName + " AS VARCHAR(254) UTF8)");
                try (final ResultSet result = statement.executeQuery(virtualSchemaQuery)) {
                    assertThat(result, table().row("2").matches(TypeMatchMode.STRICT));
                }
            });
        }
    }

    @Nested
    @TestInstance(PER_CLASS)
    @Tag("WithBooleanParameter") // workaround since IDEA can find the test otherwise
    class WithBooleanParameter {
        private ScalarFunctionQueryBuilder queryBuilder;
        private String columnName;

        @BeforeAll
        void beforeAll() throws SQLException {
            final String tableName = createBooleanVirtualSchemaTableWithSoleTrueValue();
            this.queryBuilder = new ScalarFunctionQueryBuilder(tableName);
            this.columnName = getColumnsOfTable(tableName).get(0);
        }

        @AfterAll
        void afterAll() {
            deleteCreatedTables();
        }

        @Test
        void testNeg() {
            if (isTestDisabled(NEG))
                return;
            final String query = "NOT " + this.columnName;
            runOnExasol(statement -> {
                try (final ResultSet virtualSchemaTableResult = statement
                        .executeQuery(this.queryBuilder.buildQueryFor(query))) {
                    assertThat(virtualSchemaTableResult, table().row(false).matches());
                }
            });
        }
    }

    @Nested
    @TestInstance(PER_CLASS)
    @Tag("WithStringParameter") // workaround since IDEA can find the test otherwise
    class WithStringParameter {
        private ScalarFunctionQueryBuilder queryBuilder;
        private String columnName;
        private StringTestTable.ValueSetter tableRowValueSetter;

        @BeforeAll
        void beforeAll() throws SQLException {
            final StringTestTable table = createStringVirtualSchemaTable();
            this.tableRowValueSetter = table.getValueSetter();
            this.queryBuilder = new ScalarFunctionQueryBuilder(table.getFullyQualifiedName());
            this.columnName = getColumnsOfTable(table.getFullyQualifiedName()).get(0);
        }

        @AfterAll
        void afterAll() {
            deleteCreatedTables();
        }

        /**
         * Test the SQL CASE statement.
         *
         * @implNote This test does some unnecessary math on a column of the virtual schema to make sure that this case
         *           statement is sent to the virtual schema if possible and not evaluated before. If, however, the
         *           virtual schema does not have the FLOAT_DIV or ADD capability this does not work.
         *
         * @param input          input value
         * @param expectedResult expected output
         */
        @ParameterizedTest
        @CsvSource({ //
                "a, 1", //
                "b, 2", //
                "c, 3" //
        })
        void testCase(final String input, final int expectedResult) throws SQLException {
            if (isTestDisabled(CASE))
                return;
            this.tableRowValueSetter.setValueOfSoleRow(input);
            runOnExasol(statement -> {
                final String virtualSchemaQuery = this.queryBuilder
                        .buildQueryFor("CASE " + this.columnName + " WHEN 'a' THEN 1 WHEN 'b' THEN 2 ELSE 3 END");
                try (final ResultSet result = statement.executeQuery(virtualSchemaQuery)) {
                    assertThat(result, table().row(expectedResult).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
                }
            });
        }

        @Test
        void testConvertTz() throws SQLException, ParseException {
            if (isTestDisabled(CONVERT_TZ))
                return;
            this.tableRowValueSetter.setValueOfSoleRow("UTC");
            final Timestamp expected = new Timestamp(
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2012-03-25 04:30:00").getTime());
            runOnExasol(statement -> {
                final String virtualSchemaQuery = this.queryBuilder.buildQueryFor(
                        "CONVERT_TZ(TIMESTAMP '2012-03-25 02:30:00' , " + this.columnName + ", 'Europe/Berlin')");
                try (final ResultSet result = statement.executeQuery(virtualSchemaQuery)) {
                    assertThat(result, table().row(expected).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
                }
            });
        }

        @Test
        void testDateTrunc() throws SQLException, ParseException {
            if (isTestDisabled(DATE_TRUNC))
                return;
            this.tableRowValueSetter.setValueOfSoleRow("month");
            final Timestamp expected = new Timestamp(
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2006-12-01 00:00:00.0").getTime());
            runOnExasol(statement -> {
                final String virtualSchemaQuery = this.queryBuilder
                        .buildQueryFor("DATE_TRUNC(" + this.columnName + ", TIMESTAMP '2006-12-31 23:59:59')");
                try (final ResultSet result = statement.executeQuery(virtualSchemaQuery)) {
                    assertThat(result, table().row(expected).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
                }
            });
        }

        @Test
        void testNumToDsInterval() throws SQLException {
            if (isTestDisabled(NUMTODSINTERVAL))
                return;
            this.tableRowValueSetter.setValueOfSoleRow("HOUR");
            runOnExasol(statement -> {
                final String virtualSchemaQuery = this.queryBuilder
                        .buildQueryFor("NUMTODSINTERVAL(3.2, " + this.columnName + ")");
                try (final ResultSet result = statement.executeQuery(virtualSchemaQuery)) {
                    assertThat(result,
                            table().row("+000000000 03:12:00.000000000").matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
                }
            });
        }

        @Test
        void testNumToYmInterval() throws SQLException {
            if (isTestDisabled(NUMTOYMINTERVAL))
                return;
            this.tableRowValueSetter.setValueOfSoleRow("YEAR");
            runOnExasol(statement -> {
                final String virtualSchemaQuery = this.queryBuilder
                        .buildQueryFor("NUMTOYMINTERVAL(3.5, " + this.columnName + ")");
                try (final ResultSet result = statement.executeQuery(virtualSchemaQuery)) {
                    assertThat(result, table().row("+000000003-06").matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
                }
            });
        }

        @Test
        void testToYmInterval() throws SQLException {
            if (isTestDisabled(TO_YMINTERVAL))
                return;
            this.tableRowValueSetter.setValueOfSoleRow("3-11");
            runOnExasol(statement -> {
                final String virtualSchemaQuery = this.queryBuilder
                        .buildQueryFor("TO_YMINTERVAL(" + this.columnName + ")");
                try (final ResultSet result = statement.executeQuery(virtualSchemaQuery)) {
                    assertThat(result, table().row("+000000003-11").matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
                }
            });
        }

        @Test
        void testToDsInterval() throws SQLException {
            if (isTestDisabled(TO_DSINTERVAL))
                return;
            this.tableRowValueSetter.setValueOfSoleRow("3 10:59:59.123");
            runOnExasol(statement -> {
                final String virtualSchemaQuery = this.queryBuilder
                        .buildQueryFor("TO_DSINTERVAL(" + this.columnName + ")");
                try (final ResultSet result = statement.executeQuery(virtualSchemaQuery)) {
                    assertThat(result,
                            table().row("+000000003 10:59:59.123000000").matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
                }
            });
        }

        @Test
        void testJsonValue() throws SQLException {
            if (isTestDisabled(JSON_VALUE))
                return;
            this.tableRowValueSetter.setValueOfSoleRow("{\"name\" : \"Test\"}");
            runOnExasol(statement -> {
                final String virtualSchemaQuery = this.queryBuilder
                        .buildQueryFor("JSON_VALUE(" + this.columnName + ", '$.name')");
                try (final ResultSet result = statement.executeQuery(virtualSchemaQuery)) {
                    assertThat(result, table().row("Test").matches());
                }
            });
        }
    }

    @Nested
    @TestInstance(PER_CLASS)
    @Tag("WithTimestampParameter") // workaround since IDEA can find the test otherwise
    class WithTimestampParameter {
        private ScalarFunctionQueryBuilder queryBuilder;
        private String columnName;

        @BeforeAll
        void beforeAll() throws SQLException {
            final String tableName = createDateVirtualSchemaTableWithSoleValue(new Timestamp(1000));
            this.queryBuilder = new ScalarFunctionQueryBuilder(tableName);
            this.columnName = getColumnsOfTable(tableName).get(0);
        }

        @AfterAll
        void afterAll() {
            deleteCreatedTables();
        }

        @Test
        void testExtract() {
            if (isTestDisabled(EXTRACT))
                return;
            runOnExasol(statement -> {
                final String virtualSchemaQuery = this.queryBuilder
                        .buildQueryFor("EXTRACT(MONTH FROM " + this.columnName + ")");
                try (final ResultSet result = statement.executeQuery(virtualSchemaQuery)) {
                    assertThat(result, table().row(1).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
                }
            });
        }

        @ParameterizedTest
        @CsvSource({ //
                "UTC, 1", //
                "Europe/Berlin, -3599",//
        })
        void testPosixTime(final String timeZone, final long expectedResult) {
            if (isTestDisabled(POSIX_TIME))
                return;
            runOnExasol(statement -> {
                statement.executeUpdate("ALTER SESSION SET TIME_ZONE='" + timeZone + "';");
                final String virtualSchemaQuery = this.queryBuilder
                        .buildQueryFor("POSIX_TIME(" + this.columnName + ")");
                try (final ResultSet result = statement.executeQuery(virtualSchemaQuery)) {
                    assertThat(result, table().row(expectedResult).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
                }
            });
        }
    }
}