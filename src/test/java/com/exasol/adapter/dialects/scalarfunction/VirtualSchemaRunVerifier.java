package com.exasol.adapter.dialects.scalarfunction;

import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.hamcrest.Matcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.errorreporting.ExaError;
import com.exasol.matcher.CellMatcherFactory;
import com.exasol.matcher.TypeMatchMode;

/**
 * This class checks if the scalar function produces the same result than it did on a local exasol table.
 */
public class VirtualSchemaRunVerifier {
    private static final ScalarFunctionCallBuilder CALL_BUILDER = new ScalarFunctionCallBuilder();
    private static final int BATCH_SIZE = 500;
    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualSchemaRunVerifier.class);
    private final ScalarFunctionQueryBuilder virtualSchemaQueryBuilder;

    public VirtualSchemaRunVerifier(final ScalarFunctionQueryBuilder virtualSchemaQueryBuilder) {
        this.virtualSchemaQueryBuilder = virtualSchemaQueryBuilder;
    }

    /**
     * Assert that the function behaves same on the virtual schema as it died on the Exasol table.
     *
     * @param runsOnExasol Exasol runs (parameter - result pairs) to compare to
     * @param statement    statement to use.
     */
    public List<String> assertFunctionBehavesSameOnVirtualSchema(final ScalarFunctionCapability function,
            final List<ExasolRun> runsOnExasol, final Statement statement) {
        final List<String> successParameters = new ArrayList<>();
        final List<String> failedQueries = new ArrayList<>();
        for (final ExasolRun exasolRun : runsOnExasol) {
            final String virtualSchemaQuery = this.virtualSchemaQueryBuilder
                    .buildQueryFor(CALL_BUILDER.buildFunctionCall(function, exasolRun.getParameters()));
            assertSingleRunBehavesSameOnVirtualSchema(statement, successParameters, failedQueries, exasolRun,
                    virtualSchemaQuery);
        }
        if (successParameters.isEmpty()) {
            fail(ExaError.messageBuilder("E-PGVS-15").message(
                    "Non of the combinations that worked on a native Exasol table worked on the Virtual Schema table. Here is what was tried:\n{{queries}}")
                    .unquotedParameter("queries", String.join("\n", failedQueries)).toString());
        }
        return successParameters;
    }

    private void assertSingleRunBehavesSameOnVirtualSchema(final Statement statement,
            final List<String> successParameters, final List<String> failedQueries, final ExasolRun exasolRun,
            final String virtualSchemaQuery) {
        try (final ResultSet actualResult = statement.executeQuery(virtualSchemaQuery)) {
            // check if the results are equal; Otherwise abort - wrong results are unacceptable
            try {
                assertThat(actualResult, table().row(buildMatcher(exasolRun.getResult())).matches());
            } catch (final AssertionError assertionError) {
                throw new IllegalStateException(
                        ExaError.messageBuilder("E-PGVS-13").message("Different output for query {{query}}")
                                .parameter("query", virtualSchemaQuery).toString(),
                        assertionError);
            }

            successParameters.add(exasolRun.getParameters());
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
    boolean quickCheckIfFunctionBehavesSameOnVs(final ScalarFunctionCapability function,
            final List<ExasolRun> runsOnExasol, final Statement statement) {
        for (int batchNr = 0; batchNr * BATCH_SIZE < runsOnExasol.size(); batchNr++) {
            final List<ExasolRun> batch = runsOnExasol.subList(batchNr * BATCH_SIZE,
                    Math.min(runsOnExasol.size(), (batchNr + 1) * BATCH_SIZE));
            final String selectList = batch.stream()
                    .map(run -> CALL_BUILDER.buildFunctionCall(function, run.getParameters()))
                    .collect(Collectors.joining(", "));
            final String virtualSchemaQuery = this.virtualSchemaQueryBuilder.buildQueryFor(selectList);
            try (final ResultSet actualResult = statement.executeQuery(virtualSchemaQuery)) {
                if (!table().row(batch.stream().map(ExasolRun::getResult).map(this::buildMatcher).toArray()).matches()
                        .matches(actualResult)) {
                    return false;
                }
                LOGGER.debug("Quick check query was successful: {{}}", virtualSchemaQuery);
            } catch (final SQLException exception) {
                return false;
            }
        }
        LOGGER.debug("Quick check was successful");
        return true;
    }

    private Matcher<Object> buildMatcher(final Object object) {
        return CellMatcherFactory.cellMatcher(object, TypeMatchMode.NO_JAVA_TYPE_CHECK, BigDecimal.valueOf(0.0001));
    }
}
