package com.exasol.adapter.dialects;

import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.matcher.CellMatcherFactory;
import com.exasol.matcher.TypeMatchMode;

/**
 * This is an abstract smoke test for all scalar functions.
 * <p>
 * It automatically finds parameters combinations by permuting all possible literals. Then it verifies that all
 * parameter combinations that did not cause an exception in on a regular Exasol table, behave the same on a virtual
 * schema table.
 * </p>
 * <p>
 * By default JUnit creates on instance of this class per test method. Using the {@code @TestInstance(PER_CLASS)}
 * annotation we change this behaviour to one method per class. That allows us to make the beforeAll method non static
 * and by that use abstract methods in it. By that we can create one virtual schema for all runs, which saves a lot of
 * time.
 * </p>
 * <p>
 * Do NOT define a beforeAll method in the specifc implementation! Do the initialization in {@link #setupDatabase()}
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

    /**
     * These functions are tested separately in {@link #testFunctionsWithNoParenthesis(ScalarFunctionCapability)}
     */
    private static final Set<ScalarFunctionCapability> FUNCTIONS_WITH_NO_PARENTHESIS = Set.of(LOCALTIMESTAMP, SYSDATE,
            CURRENT_SCHEMA, CURRENT_STATEMENT, CURRENT_SESSION, CURRENT_DATE, CURRENT_USER, CURRENT_TIMESTAMP);

    /**
     * For sum functions we just can guess the parameters because they only work for very specific input. For those we
     * define explicit parameters here. You can define multiple parameter combinations here (by adding multiple list
     * entries). By that you can test a function with different values.
     */
    private static final Map<ScalarFunctionCapability, List<String>> EXPLICIT_PARAMETERS = Map.of(//
            TO_YMINTERVAL, List.of("'3-11'"), //
            NUMTODSINTERVAL, List.of("3.2, 'HOUR'"), //
            NUMTOYMINTERVAL, List.of("3.5, 'YEAR'"), //
            CONVERT_TZ, List.of("TIMESTAMP '2012-05-10 12:00:00', 'UTC', 'Europe/Berlin'"), //
            JSON_VALUE, List.of("'{\"name\" : \"Smith\"}', '$.name'"), //
            EXTRACT,
            List.of("SECOND FROM TIMESTAMP '2000-10-01 12:22:59.123'", "MONTH FROM TIMESTAMP '2000-10-01 12:22:59.123'",
                    "DAY FROM TIMESTAMP '2000-10-01 12:22:59.123'"), //
            DATE_TRUNC, List.of("'month', DATE '2006-12-31'", "'day', DATE '2006-12-31'"), //
            POSIX_TIME, List.of("'1970-01-01 00:00:01'"), //
            ST_TRANSFORM, List.of("CAST('POINT(1 2)' AS GEOMETRY (4326)), 2163"), //
            CAST, List.of("'1' as INTEGER")// TODO add a better test here
    );

    /**
     * Some functions have different names than their capabilities. For that we define a mapping here. If a function is
     * not listed, we assume, it has the same name as it's capability.
     */
    private static final Map<ScalarFunctionCapability, String> RENAME_FUNCTIONS = Map.of(MIN_SCALE, "MIN", //
            NEG, "!"//
    );

    private static final int MAX_NUM_PARAMETERS = 4;
    private static final List<String> LITERALS = List.of("0.5", "2", "TRUE", "'a'", "DATE '2007-03-31'",
            "TIMESTAMP '2007-03-31 12:59:30.123'", "INTERVAL '1 12:00:30.123' DAY TO SECOND", "'POINT (1 2)'",
            "'LINESTRING (0 0, 0 1, 1 1)'", "'GEOMETRYCOLLECTION(POINT(2 5), POINT(3 5))'",
            "'POLYGON((5 1, 5 5, 9 7, 10 1, 5 1),(6 2, 6 3, 7 3, 7 2, 6 2))'",
            "'MULTIPOLYGON(((0 0, 0 2, 2 2, 3 1, 0 0)), ((4 6, 8 9, 12 5, 4 6), (8 6, 9 6, 9 7, 8 7, 8 6)))'",
            "'MULTILINESTRING((0 1, 2 3, 1 6), (4 4, 5 5))'");

    /**
     * one list for each level of parameters, that contains a list of parameter permutations. Each parameter permutation
     * is a string with comma separated parameters.
     */
    private static final List<List<String>> PARAMETER_COMBINATIONS = generateParameterCombinations();
    private static final int BATCH_SIZE = 500;

    private Supplier<Connection> connectionSupplier;
    private String fullyQualifiedNameOfArbitraryVsTable;

    static Stream<Arguments> getScalarFunctions() {
        return Arrays.stream(ScalarFunctionCapability.values())//
                .filter(function -> !EXCLUDES.contains(function) && !FUNCTIONS_WITH_NO_PARENTHESIS.contains(function))//
                .map(Arguments::of);
    }

    /**
     * Permute the literals.
     * 
     * @return permutations
     */
    private static List<List<String>> generateParameterCombinations() {
        final List<List<String>> combinations = new ArrayList<>(MAX_NUM_PARAMETERS + 1);
        combinations.add(List.of(""));
        for (int numParameters = 1; numParameters <= MAX_NUM_PARAMETERS; numParameters++) {
            final List<String> previousIterationParameters = combinations.get(numParameters - 1);
            combinations.add(previousIterationParameters.stream().flatMap(smallerCombination -> //
            LITERALS.stream()
                    .map(literal -> smallerCombination.isEmpty() || literal.isEmpty() ? smallerCombination + literal
                            : smallerCombination + ", " + literal)//
            ).collect(Collectors.toList()));
        }
        return combinations;
    }

    @BeforeAll
    final void beforeAll() {
        final Setup setup = setupDatabase();
        this.connectionSupplier = setup.connectionToExasol;
        this.fullyQualifiedNameOfArbitraryVsTable = setup.fullyQualifiedNameOfArbitraryVsTable;
    }

    protected abstract Setup setupDatabase();

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
                throw new IllegalStateException("Non of the parameter combinations lead to a successful run.");
            } else {
                assertFunctionBehavesSameOnVs(successfulExasolRuns, statement);
            }
        });
    }

    private void runOnExasol(final ExasolExecutable exasolExecutable) {
        try (final Connection connection = this.connectionSupplier.get();
                final Statement statement = connection.createStatement()) {
            exasolExecutable.runOnExasol(statement);
        } catch (final SQLException exception) {
            throw new IllegalStateException("Error during testScalarFunctions.", exception);
        }
    }

    private List<ExasolRun> findOrGetFittingParameters(final ScalarFunctionCapability function,
            final Statement statement) {
        if (EXPLICIT_PARAMETERS.containsKey(function)) {
            return findFittingParameters(function, EXPLICIT_PARAMETERS.get(function).stream(), statement);
        } else {
            return findFittingParameters(function, statement);
        }
    }

    /**
     * Try to find fitting parameters combinations by permuting the Literals and try if try if they produce an exception
     * on Exasol.
     * 
     * @param function  function to test
     * @param statement exasol statement
     * @return list of successful runs
     */
    private List<ExasolRun> findFittingParameters(final ScalarFunctionCapability function, final Statement statement) {
        final int fastThreshold = 3; // with three parameters the search is still fast; with 4 it gets slow
        final List<List<String>> fastCombinationLists = PARAMETER_COMBINATIONS.subList(0, fastThreshold + 1);
        final List<ExasolRun> fastParameters = findFittingParameters(function,
                fastCombinationLists.stream().flatMap(Collection::stream), statement);
        if (!fastParameters.isEmpty()) {
            return fastParameters;
        } else {
            for (int numParameters = fastThreshold + 1; numParameters <= MAX_NUM_PARAMETERS; numParameters++) {
                final List<ExasolRun> result = findFittingParameters(function,
                        PARAMETER_COMBINATIONS.get(numParameters).stream(), statement);
                if (!result.isEmpty()) {
                    return result;
                }
            }
            return Collections.emptyList();
        }
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
        try (final ResultSet expectedResult = statement.executeQuery("SELECT " + functionCall + " FROM DUAL")) {
            expectedResult.next();
            return new ExasolRun(functionCall, expectedResult.getObject(1));
        } catch (final SQLException exception) {
            return null;
        }
    }

    /**
     * Assert that the function behaves same on the virtual schema as it died on the Exasol table.
     * 
     * @implNote The testing is executed in batches, since some databases have a limit in the amount of columns that can
     *           be queried in a singel query.
     * 
     * @param runsOnExasol Exasol runs (parameter - result pairs) to compare to
     * @param statement    statement to use.
     */
    private void assertFunctionBehavesSameOnVs(final List<ExasolRun> runsOnExasol, final Statement statement) {
        for (int batchNr = 0; batchNr * BATCH_SIZE < runsOnExasol.size(); batchNr++) {
            final List<ExasolRun> batch = runsOnExasol.subList(batchNr * BATCH_SIZE,
                    Math.min(runsOnExasol.size(), (batchNr + 1) * BATCH_SIZE));
            final String selectList = batch.stream().map(run -> run.functionCall).collect(Collectors.joining(", "));
            final String virtualSchemaQuery = getVirtualSchemaQuery(selectList);
            try (final ResultSet actualResult = statement.executeQuery(virtualSchemaQuery)) {
                assertThat(actualResult, table()
                        .row(batch.stream().map(ExasolRun::getResult).map(this::buildMatcher).toArray()).matches());
            } catch (final SQLException exception) {
                fail("Virtual Schema query failed while Exasol query did not. (query: " + virtualSchemaQuery + ")",
                        exception);
            }
        }
    }

    private String getVirtualSchemaQuery(final String functionCall) {
        return "SELECT " + functionCall + " FROM " + this.fullyQualifiedNameOfArbitraryVsTable;
    }

    private String getFunctionName(final ScalarFunctionCapability function) {
        if (RENAME_FUNCTIONS.containsKey(function)) {
            return RENAME_FUNCTIONS.get(function);
        } else {
            return function.name();
        }
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
            final int numRuns = 100;
            final String query = getVirtualSchemaQuery("RAND()" + ", RAND()".repeat(numRuns - 1));
            try (final ResultSet result = statement.executeQuery(query)) {
                final Set<Double> results = new HashSet<>();
                for (int columnIndex = 0; columnIndex < numRuns; columnIndex++) {
                    final double value = result.getDouble(columnIndex);
                    assertThat(value, both(greaterThanOrEqualTo(0.0)).and(lessThan(1.0)));
                    results.add(value);
                }
                final int threshold = (int) (numRuns * 0.2);
                assertThat(
                        numRuns + " runs of rand should produce more then " + threshold
                                + " different results. Everything else is very improbable.",
                        results.size(), greaterThan(numRuns));
            } catch (final SQLException exception) {
                fail("Virtual schema query failed (query: " + query + ").", exception);
            }
        });
    }

    @Test
    void testSysGUID() {
        runOnExasol(statement -> {
            assertDoesNotThrow(() -> statement.executeQuery(getVirtualSchemaQuery("SYS_GUID()")).close());
        });
    }

    @Test
    void testAdd() {
        runOnExasol(statement -> {
            try (final ResultSet result = statement.executeQuery(getVirtualSchemaQuery("1 + 3"))) {
                assertThat(result, table().row(4).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
            }
        });
    }

    @Test
    void testSub() {
        runOnExasol(statement -> {
            try (final ResultSet result = statement.executeQuery(getVirtualSchemaQuery("3 - 1"))) {
                assertThat(result, table().row(2).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
            }
        });
    }

    @Test
    void testDiv() {
        runOnExasol(statement -> {
            try (final ResultSet result = statement.executeQuery(getVirtualSchemaQuery("3 / 2"))) {
                assertThat(result, table().row(1.5).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
            }
        });
    }

    @Test
    void testMult() {
        runOnExasol(statement -> {
            try (final ResultSet result = statement.executeQuery(getVirtualSchemaQuery("4 * 2"))) {
                assertThat(result, table().row(8).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
            }
        });
    }

    @Test
    void testNeg() {
        runOnExasol(statement -> {
            try (final ResultSet result = statement.executeQuery(getVirtualSchemaQuery("NOT TRUE"))) {
                assertThat(result, table().row(false).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
            }
        });
    }

    @Test
    void testSystimestamp() {
        runOnExasol(statement -> {
            try (final ResultSet result = statement.executeQuery(getVirtualSchemaQuery("SYSTIMESTAMP"))) {
                result.next();
                final Date date = result.getDate(1);
                final long difference = Math.abs(date.getTime() - new java.util.Date().getTime());
                assertThat("difference is less than 5 minutes.", difference, lessThan(5 * 60 * 1000L));
            }
        });
    }

    @ParameterizedTest
    @CsvSource({ //
            "1, a", //
            "2, b", //
            "5, c",//
    })
    void testCase(final int input, final String expectedResult) {
        runOnExasol(statement -> {
            try (final ResultSet result = statement.executeQuery(
                    getVirtualSchemaQuery("CASE " + input + " WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END"))) {
                assertThat(result, table().row(expectedResult).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
            }
        });
    }

    @FunctionalInterface
    private interface ExasolExecutable {
        void runOnExasol(Statement statement) throws SQLException;
    }

    protected static class Setup {
        private final String fullyQualifiedNameOfArbitraryVsTable;
        private final Supplier<Connection> connectionToExasol;

        /**
         * Create a new instance of {@link Setup}
         * 
         * @param fullyQualifiedNameOfArbitraryVsTable the fully qualified name of an arbitrary table in a virtual
         *                                             schema. The table might be empty. The table is only use to cause
         *                                             the Exasol database to invoke the virtual schema. The actual data
         *                                             is sent to the functions using literals.
         * @param connectionToExasol                   sql connection supplier for the Exasol database
         */
        public Setup(final String fullyQualifiedNameOfArbitraryVsTable, final Supplier<Connection> connectionToExasol) {
            this.fullyQualifiedNameOfArbitraryVsTable = fullyQualifiedNameOfArbitraryVsTable;
            this.connectionToExasol = connectionToExasol;
        }
    }

    private static class ExasolRun {
        private final String functionCall;
        private final Object result;

        private ExasolRun(final String functionCall, final Object result) {
            this.functionCall = functionCall;
            this.result = result;
        }

        public Object getResult() {
            return this.result;
        }
    }
}
