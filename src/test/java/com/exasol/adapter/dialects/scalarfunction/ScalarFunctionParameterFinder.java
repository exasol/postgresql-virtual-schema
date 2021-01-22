package com.exasol.adapter.dialects.scalarfunction;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exasol.adapter.capabilities.ScalarFunctionCapability;

/**
 * This class searches for parameter combinations that do not cause an exception on a local exasol table.
 */
public class ScalarFunctionParameterFinder {

    private static final ScalarFunctionCallBuilder FUNCTION_CALL_BUILDER = new ScalarFunctionCallBuilder();
    private static final Logger LOGGER = LoggerFactory.getLogger(ScalarFunctionParameterFinder.class);
    private static final int MAX_NUM_PARAMETERS = 4;
    /**
     * One list for each level of parameters, that contains a list of parameter permutations. Each parameter permutation
     * is a string with comma separated parameters.
     */
    private final List<List<String>> parameterCombinations;
    private final ScalarFunctionQueryBuilder localQueryBuilder;
    private final ScalarFunctionsParameterCache parameterCache;

    public ScalarFunctionParameterFinder(final Collection<String> availableColumns,
            final ScalarFunctionQueryBuilder localQueryBuilder, final ScalarFunctionsParameterCache parameterCache) {
        this.parameterCombinations = generateParameterCombinations(availableColumns);
        this.localQueryBuilder = localQueryBuilder;
        this.parameterCache = parameterCache;
    }

    /**
     * Permute the literals.
     *
     * @return permutations
     */
    private static List<List<String>> generateParameterCombinations(final Collection<String> availableColumns) {
        final List<List<String>> combinations = new ArrayList<>(MAX_NUM_PARAMETERS + 1);
        combinations.add(List.of(""));
        for (int numParameters = 1; numParameters <= MAX_NUM_PARAMETERS; numParameters++) {
            final List<String> previousIterationParameters = combinations.get(numParameters - 1);
            combinations.add(previousIterationParameters.stream().flatMap(smallerCombination -> //
            availableColumns.stream().map(literal -> joinWithCommaIfNecessary(smallerCombination, literal))//
            ).collect(Collectors.toList()));
        }
        return combinations;
    }

    private static String joinWithCommaIfNecessary(final String smallerCombination, final String literal) {
        if (smallerCombination.isEmpty() || literal.isEmpty()) {
            return smallerCombination + literal;
        } else {
            return smallerCombination + ", " + literal;
        }
    }

    public List<ExasolRun> findOrGetFittingParameters(final ScalarFunctionCapability function,
            final Statement statement) {
        if (this.parameterCache.hasParametersForFunction(function.name())) {
            LOGGER.debug("Using parameters from parameter cache for function {}.", function.name());
            return findFittingParameters(function,
                    this.parameterCache.getFunctionsValidParameterCombinations(function.name()).stream(), statement);
        } else {
            LOGGER.debug("Using generated parameters for function {}.", function.name());
            return findFittingParameters(function, statement);
        }
    }

    /**
     * Try to find fitting parameters combinations by permuting a set of columns and try if they produce an exception on
     * a regular Exasol table.
     * <p>
     * This method first tries to find fitting combinations with only 3 columns and only if that has no results,
     * increases the number of columns (which leads to an exponential growth of possible combinations).
     * </p>
     *
     * @param function  function to test
     * @param statement exasol statement
     * @return list of successful runs
     */
    private List<ExasolRun> findFittingParameters(final ScalarFunctionCapability function, final Statement statement) {
        final int fastThreshold = 3; // with three parameters the search is still fast; with 4 it gets slow
        final List<List<String>> fastCombinationLists = this.parameterCombinations.subList(0, fastThreshold + 1);
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
                    this.parameterCombinations.get(numParameters).stream(), statement);
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
        final String functionCall = FUNCTION_CALL_BUILDER.buildFunctionCall(function, parameters);
        try (final ResultSet expectedResult = statement
                .executeQuery(this.localQueryBuilder.buildQueryFor(functionCall))) {
            expectedResult.next();
            return new ExasolRun(parameters, expectedResult.getObject(1));
        } catch (final SQLException exception) {
            return null;
        }
    }
}
