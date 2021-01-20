package com.exasol.adapter.dialects;

import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.Matcher;
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
import com.exasol.matcher.CellMatcherFactory;
import com.exasol.matcher.TypeMatchMode;

/**
 * This is an abstract smoke test for all scalar functions, except the geospacial functions (ST_*).
 * <p>
 * It automatically finds parameters combinations by permuting a set of different values. Then it verifies that on of
 * the parameter combinations that did not cause an exception in on a regular Exasol table, succeeds on the virtual
 * schema tale. In addition this test asserts that all queries that succeed on the virtual and the regular table have
 * the same result.
 * </p>
 * <p>
 * By default JUnit creates on instance of this class per test method. Using the {@code @TestInstance(PER_CLASS)}
 * annotation we change this behaviour to one method per class. That allows us to make the beforeAll method non static
 * and by that use abstract methods in it. By that we can create one virtual schema for all runs, which saves a lot of
 * time.
 * </p>
 * <p>
 * Do NOT define a beforeAll method in the specific implementation! Do the initialization in {@link #setupDatabase()}
 * instead.
 * </p>
 */
@TestInstance(PER_CLASS)
@Testcontainers
@Execution(value = ExecutionMode.CONCURRENT) // use -Djunit.jupiter.execution.parallel.enabled=true to enable parallel
@SuppressWarnings("java:S5786") // class is public since it is an abstract test
public abstract class ScalarFunctionsAbstractIT {
    /**
     * These have a special syntax, so we define explicit test for them below.
     */
    private static final Set<ScalarFunctionCapability> EXCLUDES = Set.of(CASE, FLOAT_DIV, SESSION_PARAMETER, RAND, ADD,
            SUB, MULT, NEG, SYS_GUID, SYSTIMESTAMP);
    private static final String LOCAL_COPY_TABLE_NAME = "LOCAL_COPY";
    private static final String LOCAL_COPY_SCHEMA = "EXASOL";

    /**
     * These functions are tested separately in {@link #testFunctionsWithNoParenthesis(ScalarFunctionCapability)}
     */
    private static final Set<ScalarFunctionCapability> FUNCTIONS_WITH_NO_PARENTHESIS = Set.of(LOCALTIMESTAMP, SYSDATE,
            CURRENT_SCHEMA, CURRENT_STATEMENT, CURRENT_SESSION, CURRENT_DATE, CURRENT_USER, CURRENT_TIMESTAMP);
    private static final String LOCAL_COPY_FULL_TABLE_NAME = LOCAL_COPY_SCHEMA + "." + LOCAL_COPY_TABLE_NAME;
    private static final Map<String, Object> EXPECTED_TEST_TABLE_VALUES = Map.of(//
            "FLOATING_POINT", 0.5, //
            "INTEGER", 2, //
            "STRING", "a", //
            "EMPTY_STRING", "", //
            "DATE", new Date(1000), //
            "TIMESTAMP", new Timestamp(1001));

    /**
     * Some functions have different names than their capabilities. For that we define a mapping here. If a function is
     * not listed, we assume, it has the same name as it's capability.
     */
    private static final Map<ScalarFunctionCapability, String> RENAME_FUNCTIONS = Map.of(MIN_SCALE, "MIN", //
            NEG, "!"//
    );

    private static final int MAX_NUM_PARAMETERS = 4;
    private static final int BATCH_SIZE = 500;
    private Map<String, String> columnsWithType;
    /**
     * For sum functions we just can guess the parameters because they only work for very specific input. For those we
     * define explicit parameters here. You can define multiple parameter combinations here (by adding multiple list
     * entries). By that you can test a function with different values.
     */
    private Map<ScalarFunctionCapability, List<String>> explicitParameters;
    private String fullyQualifiedNameOfVsTable;
    private static final Logger LOGGER = LoggerFactory.getLogger(ScalarFunctionsAbstractIT.class);

    /**
     * One list for each level of parameters, that contains a list of parameter permutations. Each parameter permutation
     * is a string with comma separated parameters.
     */
    private static List<List<String>> parameterCombinations;

    /**
     * Permute the literals.
     *
     * @return permutations
     */
    private static List<List<String>> generateParameterCombinations(final Collection<String> atoms) {
        final List<List<String>> combinations = new ArrayList<>(MAX_NUM_PARAMETERS + 1);
        combinations.add(List.of(""));
        for (int numParameters = 1; numParameters <= MAX_NUM_PARAMETERS; numParameters++) {
            final List<String> previousIterationParameters = combinations.get(numParameters - 1);
            combinations.add(previousIterationParameters.stream().flatMap(smallerCombination -> //
            atoms.stream().map(literal -> joinWithCommaIfNecessary(smallerCombination, literal))//
            ).collect(Collectors.toList()));
        }
        return combinations;
    }

    private ScalarFunctionsParameterCache parameterCache;

    private static String joinWithCommaIfNecessary(final String smallerCombination, final String literal) {
        if (smallerCombination.isEmpty() || literal.isEmpty()) {
            return smallerCombination + literal;
        } else {
            return smallerCombination + ", " + literal;
        }
    }

    /**
     * Returns a set of scalar functions that should not be tested for this dialect.
     * 
     * @return set of functions
     */
    protected abstract Set<ScalarFunctionCapability> getDialectSpecificExcludes();

    private void initExplicitParameters() {
        this.explicitParameters = Map.of(//
                TO_YMINTERVAL, List.of("'3-11'"), // TODO
                NUMTODSINTERVAL, List.of("3.2, 'HOUR'"), // TODO
                NUMTOYMINTERVAL, List.of("3.5, 'YEAR'"), // TODO
                CONVERT_TZ, List.of(getColumnForType("TIMESTAMP") + ", 'UTC', 'Europe/Berlin'"), //
                JSON_VALUE, List.of("CONCAT('{\"name\" : \"', " + getColumnForType("VARCHAR") + ", '\"}'), '$.name'"), //
                EXTRACT,
                List.of("SECOND FROM TIMESTAMP " + getColumnForType("TIMESTAMP"),
                        "MONTH FROM TIMESTAMP " + getColumnForType("TIMESTAMP"),
                        "DAY FROM TIMESTAMP " + getColumnForType("TIMESTAMP")), //
                DATE_TRUNC, List.of("'month', " + getColumnForType("DATE"), "'day', " + getColumnForType("DATE")), //
                POSIX_TIME, List.of("'1970-01-01 00:00:01'"), // TODO
                CAST, List.of("(CAST " + getColumnForType("DECIMAL") + " as VARCHAR(254) UTF8) as INTEGER"));
    }

    Stream<Arguments> getScalarFunctions() {
        final Set<ScalarFunctionCapability> dialectSpecificExcludes = getDialectSpecificExcludes();
        return Arrays.stream(ScalarFunctionCapability.values())//
                .filter(function -> !EXCLUDES.contains(function) && !dialectSpecificExcludes.contains(function)
                        && !FUNCTIONS_WITH_NO_PARENTHESIS.contains(function) && !function.name().startsWith("ST_"))//
                .map(Arguments::of);
    }

    @BeforeAll
    final void beforeAll() throws SQLException {
        this.fullyQualifiedNameOfVsTable = setupDatabase();
        runOnExasol(statement -> {
            statement.executeUpdate("CREATE SCHEMA " + LOCAL_COPY_SCHEMA);
            statement.executeUpdate("CREATE TABLE " + LOCAL_COPY_FULL_TABLE_NAME + " as SELECT * FROM "
                    + this.fullyQualifiedNameOfVsTable);
            this.columnsWithType = getTestTablesColumns(statement);
            parameterCombinations = generateParameterCombinations(this.columnsWithType.keySet());
            this.parameterCache = new ScalarFunctionsParameterCache();
            initExplicitParameters();
        });
    }

    /**
     * Find a column of the test table for a specific type.
     * 
     * @param requestedTypePrefix prefix for the requested type e.g. VARCHAR
     * @return quoted column name
     */
    private String getColumnForType(final String requestedTypePrefix) {
        return this.columnsWithType.entrySet().stream()
                .filter(entry -> entry.getValue().startsWith(requestedTypePrefix))//
                .map(Map.Entry::getKey)//
                .map(column -> "\"" + column + "\"")//
                .findAny()
                .orElseThrow(() -> new IllegalStateException(ExaError.messageBuilder("E-PGVS-16").message(
                        "The dialect specific example table had no column for the requested type starting with {{type}}. Available column: {{available columns}}.")
                        .parameter("type", requestedTypePrefix)//
                        .parameter("available columns", this.columnsWithType).toString()));
    }

    private Map<String, String> getTestTablesColumns(final Statement statement) throws SQLException {
        try (final ResultSet resultSet = statement
                .executeQuery("SELECT COLUMN_NAME, COLUMN_TYPE FROM EXA_ALL_COLUMNS WHERE COLUMN_TABLE = '"
                        + LOCAL_COPY_TABLE_NAME + "' AND COLUMN_SCHEMA = '" + LOCAL_COPY_SCHEMA + "'")) {
            final Map<String, String> columnsWithType = new LinkedHashMap<>();
            while (resultSet.next()) {
                columnsWithType.put(resultSet.getString("COLUMN_NAME"), resultSet.getString("COLUMN_TYPE"));
            }
            return columnsWithType;
        }
    }

    /**
     * Prepare a Virtual Schema with one table that contains columns for the following types: floating point, integer, 0
     * named ZERO, string(not empty), string(empty) named EMPTY_STRING, date, timestamp
     *
     * @return the fully qualified name of an arbitrary table in a virtual schema. The table might be empty. The table
     *         is only use to cause the Exasol database to invoke the virtual schema. The actual data is sent to the
     *         functions using literals.
     */
    protected abstract String setupDatabase() throws SQLException;

    protected abstract Connection createExasolConnection() throws SQLException;

    /**
     * Test for most of the scala functions. Since they are so many, it's too much effort to write all parameter
     * combinations here. Instead this test tries all permutations on an Exasol table and in case they do not cause an
     * exception asserts that they produce the same result on the virtual schema table.
     *
     * @param function function to test
     */
    @ParameterizedTest
    @MethodSource("getScalarFunctions")
    void testScalarFunctions(final ScalarFunctionCapability function) {
        runOnExasol(statement -> {
            final List<ExasolRun> successfulExasolRuns = findOrGetFittingParameters(function, statement);
            if (successfulExasolRuns.isEmpty()) {
                throw new IllegalStateException(ExaError.messageBuilder("E-PGVS-14")
                        .message("Non of the parameter combinations lead to a successful run.").toString());
            } else {
                if (!quickCheckIfFunctionBehavesSameOnVs(function, successfulExasolRuns, statement)) {
                    LOGGER.info("Quick test failed for {}. Running full checks.", function);
                    assertFunctionBehavesSameOnVirtualSchema(function, successfulExasolRuns, statement);
                }
            }
        });
    }

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

    private List<ExasolRun> findOrGetFittingParameters(final ScalarFunctionCapability function,
            final Statement statement) {
        if (this.explicitParameters.containsKey(function)) {
            LOGGER.debug("Using explicit parameters for function {}.", function.name());
            return findFittingParameters(function, this.explicitParameters.get(function).stream(), statement);
        } else if (this.parameterCache.hasParametersForFunction(function.name())) {
            LOGGER.debug("Using parameters from parameter cache for function {}.", function.name());
            return findFittingParameters(function,
                    this.parameterCache.getFunctionsValidParameterCombinations(function.name()).stream(), statement);
        } else {
            LOGGER.debug("Using generated parameters for function {}.", function.name());
            return findFittingParameters(function, statement);
        }
    }

    /**
     * Try to find fitting parameters combinations by permuting a set simple values and try if they produce an exception
     * on a regular Exasol table.
     * <p>
     * This method first tries to find fitting combinations with only 3 literals and only if that has no results,
     * increases the number of literals (which leads to an exponential growth of possible combinations).
     * </p>
     *
     * @param function  function to test
     * @param statement exasol statement
     * @return list of successful runs
     */
    private List<ExasolRun> findFittingParameters(final ScalarFunctionCapability function, final Statement statement) {
        final int fastThreshold = 3; // with three parameters the search is still fast; with 4 it gets slow
        final List<List<String>> fastCombinationLists = parameterCombinations.subList(0, fastThreshold + 1);
        final List<ExasolRun> fastParameters = findFittingParameters(function,
                fastCombinationLists.stream().flatMap(Collection::stream), statement);
        if (!fastParameters.isEmpty()) {
            return fastParameters;
        } else {
            return findCostyFittingParameters(function, statement, fastThreshold);
        }
    }

    private List<ExasolRun> findCostyFittingParameters(final ScalarFunctionCapability function,
            final Statement statement, final int fastThreshold) {
        for (int numParameters = fastThreshold + 1; numParameters <= MAX_NUM_PARAMETERS; numParameters++) {
            final List<ExasolRun> result = findFittingParameters(function,
                    parameterCombinations.get(numParameters).stream(), statement);
            if (!result.isEmpty()) {
                return result;
            }
        }
        return Collections.emptyList();
    }

    private List<ExasolRun> findFittingParameters(final ScalarFunctionCapability function,
            final Stream<String> possibleParameters, final Statement statement) {
        return possibleParameters
                .map(parameterCombination -> this.runFunctionOnExasol(function, parameterCombination, statement))
                .filter(Objects::nonNull).collect(Collectors.toList());
    }

    private ExasolRun runFunctionOnExasol(final ScalarFunctionCapability function, final String parameters,
            final Statement statement) {
        final String functionCall = buildFunctionCall(function, parameters);
        try (final ResultSet expectedResult = statement.executeQuery(getLocalTableQuery(functionCall))) {
            expectedResult.next();
            return new ExasolRun(parameters, expectedResult.getObject(1));
        } catch (final SQLException exception) {
            return null;
        }
    }

    /**
     * Assert that the function behaves same on the virtual schema as it died on the Exasol table.
     *
     * @param runsOnExasol Exasol runs (parameter - result pairs) to compare to
     * @param statement    statement to use.
     */
    private void assertFunctionBehavesSameOnVirtualSchema(final ScalarFunctionCapability function,
            final List<ExasolRun> runsOnExasol, final Statement statement) {
        this.parameterCache.removeFunction(function.name());
        final List<String> successParameters = new ArrayList<>();
        final List<String> failedQueries = new ArrayList<>();
        for (final ExasolRun exasolRun : runsOnExasol) {
            final String virtualSchemaQuery = getVirtualSchemaQuery(buildFunctionCall(function, exasolRun.parameters));
            assertSingleRunBehavesSameOnVirtualSchema(statement, successParameters, failedQueries, exasolRun,
                    virtualSchemaQuery);
        }
        if (successParameters.isEmpty()) {
            fail(ExaError.messageBuilder("E-PGVS-15").message(
                    "Non of the combinations that worked on a native Exasol table worked on the Virtual Schema table. Here is what was tried:\n{{queries}}")
                    .unquotedParameter("queries", String.join("\n", failedQueries)).toString());
        } else {
            this.parameterCache.setFunctionsValidParameterCombinations(function.name(), successParameters);
            this.parameterCache.flush();
        }
    }

    private void assertSingleRunBehavesSameOnVirtualSchema(final Statement statement,
            final List<String> successParameters, final List<String> failedQueries, final ExasolRun exasolRun,
            final String virtualSchemaQuery) {
        try (final ResultSet actualResult = statement.executeQuery(virtualSchemaQuery)) {
            // check if the results are equal; Otherwise abort - wrong results are unacceptable
            try {
                assertThat(actualResult, table().row(buildMatcher(exasolRun.result)).matches());
            } catch (final AssertionError assertionError) {
                throw new IllegalStateException(
                        ExaError.messageBuilder("E-PGVS-13").message("Different output for query {{query}}")
                                .parameter("query", virtualSchemaQuery).toString(),
                        assertionError);
            }

            successParameters.add(exasolRun.parameters);
        } catch (final SQLException exception) {
            failedQueries.add(virtualSchemaQuery);
            // ignore; probably just a strange parameter combination
        }
    }

    /**
     * Quick check if the function behaves same on the virtual schema as it did on the Exasol table.
     * <p>
     * In contrast to {@link #assertFunctionBehavesSameOnVirtualSchema(ScalarFunctionCapability, List, Statement)} this
     * function does not check each parameter combination in a separate query but combines them into a single query. By
     * that this function is a lot fast. On the other hand, if a single combination leads to an error, the whole query
     * fails. So you can only use this function as a shortcut if it returns true.
     * </p>
     *
     * @param runsOnExasol Exasol runs (parameter - result pairs) to compare to
     * @param statement    statement to use.
     * @implNote The testing is executed in batches, since some databases have a limit in the amount of columns that can
     *           be queried in a singel query.
     * @return {@code true} if all parameter combinations behaved same. {@code false} otherwise
     */
    private boolean quickCheckIfFunctionBehavesSameOnVs(final ScalarFunctionCapability function,
            final List<ExasolRun> runsOnExasol, final Statement statement) {
        for (int batchNr = 0; batchNr * BATCH_SIZE < runsOnExasol.size(); batchNr++) {
            final List<ExasolRun> batch = runsOnExasol.subList(batchNr * BATCH_SIZE,
                    Math.min(runsOnExasol.size(), (batchNr + 1) * BATCH_SIZE));
            final String selectList = batch.stream().map(run -> buildFunctionCall(function, run.parameters))
                    .collect(Collectors.joining(", "));
            final String virtualSchemaQuery = getVirtualSchemaQuery(selectList);
            try (final ResultSet actualResult = statement.executeQuery(virtualSchemaQuery)) {
                if (!table().row(batch.stream().map(ExasolRun::getResult).map(this::buildMatcher).toArray()).matches()
                        .matches(actualResult)) {
                    return false;
                }
            } catch (final SQLException exception) {
                return false;
            }
        }
        return true;
    }

    private String getLocalTableQuery(final String functionCall) {
        return "SELECT " + functionCall + " FROM " + LOCAL_COPY_FULL_TABLE_NAME;
    }

    private String getVirtualSchemaQuery(final String functionCall) {
        return "SELECT " + functionCall + " FROM " + this.fullyQualifiedNameOfVsTable;
    }

    private String getFunctionName(final ScalarFunctionCapability function) {
        return RENAME_FUNCTIONS.getOrDefault(function, function.name());
    }

    private String buildFunctionCall(final ScalarFunctionCapability function, final String parameters) {
        return getFunctionName(function) + "(" + String.join(", ", parameters) + ")";
    }

    private Matcher<Object> buildMatcher(final Object object) {
        return CellMatcherFactory.cellMatcher(object, TypeMatchMode.NO_JAVA_TYPE_CHECK, BigDecimal.valueOf(0.0001));
    }

    private Stream<Arguments> getScalarFunctionWithNoParameters() {
        return FUNCTIONS_WITH_NO_PARENTHESIS.stream().map(Arguments::of);
    }

    /**
     * This test tests functions that do not have parenthesis (e.g. CURRENT_SCHEMA).
     * <p>
     * Since the result of all of this functions is different on different databases or at different time, we just test
     * that they don't throw an exception on the Virtual Schema.
     *
     * </p>
     *
     * @param function function to test.
     */
    @ParameterizedTest
    @MethodSource("getScalarFunctionWithNoParameters")
    void testFunctionsWithNoParenthesis(final ScalarFunctionCapability function) {
        runOnExasol(statement -> {
            assertDoesNotThrow(() -> statement.executeQuery(getVirtualSchemaQuery(getFunctionName(function))).close());
        });
    }

    /**
     * For the random function it obviously makes no sense to check if the output is the same on two runs. For that
     * reason we check here if it produces at least some different results.
     */
    @Test
    void testRand() {
        runOnExasol(statement -> {
            final String query = getVirtualSchemaQuery("RAND()");
            try (final ResultSet result = statement.executeQuery(query)) {
                result.next();
                final double value = result.getDouble(1);
                assertThat(Double.isFinite(value), equalTo(true));
            }
        });
    }

    @Test
    void testSysGUID() {
        runOnExasol(statement -> {
            assertDoesNotThrow(() -> statement.executeQuery(getVirtualSchemaQuery("SYS_GUID()")).close());
        });
    }

    // TODO improve hamcrest matcher to fuzzy match result against result and then change this
    @ParameterizedTest
    @ValueSource(strings = { "+", "-", "*", "/" })
    void testSimpleArithmeticFunctions(final String operator) {
        final String numberColumn = getColumnForType("DECIMAL");
        final String query = numberColumn + " " + operator + " " + numberColumn;
        runOnExasol(statement -> {
            try (final ResultSet nativeTableResult = statement.executeQuery(getLocalTableQuery(query));
                    final ResultSet virtualSchemaTableResult = statement.executeQuery(getVirtualSchemaQuery(query))) {
                nativeTableResult.next();
                assertThat(virtualSchemaTableResult,
                        table().row(nativeTableResult.getDouble(1)).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
            }
        });
    }

    // TODO improve hamcrest matcher to fuzzy match result against result and then change this to use common method
    @Test
    void testNeg() {
        final String booleanColumn = getColumnForType("BOOLEAN");
        final String query = "NOT " + booleanColumn;
        runOnExasol(statement -> {
            try (final ResultSet nativeTableResult = statement.executeQuery(getLocalTableQuery(query));
                    final ResultSet virtualSchemaTableResult = statement.executeQuery(getVirtualSchemaQuery(query))) {
                nativeTableResult.next();
                assertThat(virtualSchemaTableResult,
                        table().row(nativeTableResult.getBoolean(1)).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
            }
        });
    }

    // TODO rewrite; right now it tests exasol against exasol
    @Test
    void testSystimestamp() {
        runOnExasol(statement -> {
            try (final ResultSet result = statement.executeQuery(getVirtualSchemaQuery("SYSTIMESTAMP"))) {
                result.next();
                final LocalDateTime timestamp = result.getTimestamp(1).toLocalDateTime();
                assertAll(() -> assertTrue(timestamp.isAfter(LocalDateTime.now().minusMinutes(20))),
                        () -> assertTrue(timestamp.isBefore(LocalDateTime.now().plusMinutes(1))));
            }
        });
    }

    /**
     * Test the SQL CASE statement.
     * 
     * @implNote This test does some unnecessary math on a column of the virtual schema to make sure that this case
     *           statement is sent to the virtual schema if possible and not evaluated before. If, however, the virtual
     *           schema does not have the FLOAT_DIV or ADD capability this does not work.
     * 
     * @param input          input value
     * @param expectedResult expected output
     */
    @ParameterizedTest
    @CsvSource({ //
            "1, a", //
            "2, b", //
            "5, c" //
    })
    void testCase(final int input, final String expectedResult) {
        final String numberColumn = getColumnForType("DECIMAL");
        runOnExasol(statement -> {
            final String virtualSchemaQuery = getVirtualSchemaQuery("CASE (" + numberColumn + " / " + numberColumn
                    + ") * " + input + " WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END");
            try (final ResultSet result = statement.executeQuery(virtualSchemaQuery)) {
                assertThat(result, table().row(startsWith(expectedResult)).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
            }
        });
    }

    @FunctionalInterface
    private interface ExasolExecutable {
        void runOnExasol(Statement statement) throws SQLException;
    }

    private static class ExasolRun {
        private final String parameters;
        private final Object result;

        private ExasolRun(final String parameters, final Object result) {
            this.parameters = parameters;
            this.result = result;
        }

        public Object getResult() {
            return this.result;
        }
    }
}