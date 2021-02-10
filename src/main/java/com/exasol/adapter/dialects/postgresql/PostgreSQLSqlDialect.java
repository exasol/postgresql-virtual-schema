package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.SQLException;
import java.util.*;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.sql.ScalarFunction;
import com.exasol.adapter.sql.SqlNodeVisitor;
import com.exasol.errorreporting.ExaError;

/**
 * This class implements the PostgreSQL dialect.
 */
public class PostgreSQLSqlDialect extends AbstractSqlDialect {
    static final String NAME = "POSTGRESQL";
    public static final String POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY = "POSTGRESQL_IDENTIFIER_MAPPING";
    public static final String POSTGRESQL_UPPERCASE_TABLES_SWITCH = "POSTGRESQL_UPPERCASE_TABLES";
    private static final String POSTGRESQL_IDENTIFER_MAPPING_PRESERVE_ORIGINAL_CASE_VALUE = "PRESERVE_ORIGINAL_CASE";
    private static final String POSTGRESQL_IDENTIFIER_MAPPING_CONVERT_TO_UPPER_VALUE = "CONVERT_TO_UPPER";
    private static final PostgreSQLIdentifierMapping DEFAULT_POSTGRESS_IDENTIFIER_MAPPING = PostgreSQLIdentifierMapping.CONVERT_TO_UPPER;
    private static final Set<ScalarFunctionCapability> DISABLED_SCALAR_FUNCTION = Set.of(
            /* implementation is very hard. See design.md. */
            SECONDS_BETWEEN, MINUTES_BETWEEN, HOURS_BETWEEN, DAYS_BETWEEN, MONTHS_BETWEEN, YEARS_BETWEEN, //
            ROUND, // PostgreSQL rounds 0.5 down while exasol rounds it up
            SECOND, // Seems to have some issues with precision
            COLOGNE_PHONETIC, // No PostgreSQL equivalent
            CONCAT, // fails for boolean since different to string behaviour
            INSTR, // not implemented; probably possible using strpos
            POSIX_TIME, // did not respect exasol session timezone
            // simply not implemented:
            DUMP, EDIT_DISTANCE, INSERT, LOCATE, REGEXP_INSTR, REGEXP_SUBSTR, SOUNDEX, SPACE, UNICODE, UNICODECHR,
            DBTIMEZONE, FROM_POSIX_TIME, HOUR, SESSIONTIMEZONE, IS_NUMBER, IS_BOOLEAN, IS_DATE, IS_DSINTERVAL,
            IS_YMINTERVAL, IS_TIMESTAMP, TO_CHAR, TO_DATE, TO_NUMBER, TO_TIMESTAMP, BIT_AND, BIT_CHECK, BIT_LROTATE,
            BIT_LSHIFT, BIT_NOT, BIT_OR, BIT_RROTATE, BIT_RSHIFT, BIT_SET, BIT_TO_NUM, BIT_XOR, HASHTYPE_MD5, HASH_SHA1,
            HASHTYPE_SHA1, HASH_SHA256, HASHTYPE_SHA256, HASH_SHA512, HASHTYPE_SHA512, HASH_TIGER, HASHTYPE_TIGER,
            NULLIFZERO, ZEROIFNULL, MIN_SCALE, NUMTOYMINTERVAL, JSON_VALUE, TO_DSINTERVAL, CONVERT_TZ, NUMTODSINTERVAL,
            TO_YMINTERVAL, CAST, SYS_GUID, SYSTIMESTAMP, CURRENT_STATEMENT, CURRENT_USER, SYSDATE, CURRENT_SESSION//
    );
    private static final Capabilities CAPABILITIES = createCapabilityList();

    /*
     * IMPORTANT! Before adding new capabilities, check the doc/design.md if there is a note why we explicitly not add
     * it.
     */
    private static Capabilities createCapabilityList() {
        return Capabilities.builder()
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE,
                        AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT, LIMIT_WITH_OFFSET, JOIN,
                        JOIN_TYPE_INNER, JOIN_TYPE_LEFT_OUTER, JOIN_TYPE_RIGHT_OUTER, JOIN_TYPE_FULL_OUTER,
                        JOIN_CONDITION_EQUI)
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN, REGEXP_LIKE,
                        IN_CONSTLIST, IS_NULL, IS_NOT_NULL)
                .addLiteral(BOOL, NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING)
                .addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT,
                        MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP, STDDEV_POP_DISTINCT,
                        STDDEV_SAMP, STDDEV_SAMP_DISTINCT, VARIANCE, VARIANCE_DISTINCT, VAR_POP, VAR_POP_DISTINCT,
                        VAR_SAMP, VAR_SAMP_DISTINCT, GROUP_CONCAT)
                .addScalarFunction(Arrays.stream(ScalarFunctionCapability.values())
                        .filter(function -> !isGeospatial(function) && !DISABLED_SCALAR_FUNCTION.contains(function))
                        .toArray(ScalarFunctionCapability[]::new))
                .build();
    }

    private static boolean isGeospatial(final ScalarFunctionCapability function) {
        return function.name().startsWith("ST_");
    }

    /**
     * Create a new instance of the {@link PostgreSQLSqlDialect}.
     *
     * @param connectionFactory factory for the JDBC connection to the remote data source
     * @param properties        user-defined adapter properties
     */
    public PostgreSQLSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        super(connectionFactory, properties, Set.of(CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, IGNORE_ERRORS_PROPERTY,
                POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY));
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        try {
            return new PostgreSQLMetadataReader(this.connectionFactory.getConnection(), this.properties);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException(ExaError.messageBuilder("E-PGVS-3")
                    .message("Unable to create PostgreSQL remote metadata reader. Caused by: " + exception.getMessage())
                    .toString(), exception);
        }
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new ImportIntoQueryRewriter(this, createRemoteMetadataReader(), this.connectionFactory);
    }

    @Override
    public boolean omitParentheses(final ScalarFunction function) {
        return function.name().equals("CURRENT_DATE") || function.name().equals("CURRENT_TIMESTAMP")
                || function.name().equals("LOCALTIMESTAMP");
    }

    @Override
    public Capabilities getCapabilities() {
        return CAPABILITIES;
    }

    private PostgreSQLIdentifierMapping getIdentifierMapping() {
        if (this.properties.containsKey(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY)) {
            return PostgreSQLIdentifierMapping.parse(this.properties.get(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY));
        } else {
            return DEFAULT_POSTGRESS_IDENTIFIER_MAPPING;
        }
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        final Map<ScalarFunction, String> scalarAliases = new EnumMap<>(ScalarFunction.class);
        scalarAliases.put(ScalarFunction.SUBSTR, "SUBSTRING");
        scalarAliases.put(ScalarFunction.HASH_MD5, "MD5");
        scalarAliases.put(ScalarFunction.RAND, "RANDOM");
        return scalarAliases;
    }

    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.MULTIPLE;
    }

    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.MULTIPLE;
    }

    @Override
    // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    public String applyQuote(final String identifier) {
        String postgreSQLIdentifier = identifier;
        if (getIdentifierMapping() != PostgreSQLIdentifierMapping.PRESERVE_ORIGINAL_CASE) {
            postgreSQLIdentifier = convertIdentifierToLowerCase(postgreSQLIdentifier);
        }
        return super.quoteIdentifierWithDoubleQuotes(postgreSQLIdentifier);
    }

    private String convertIdentifierToLowerCase(final String identifier) {
        return identifier.toLowerCase();
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return false;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        return true;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_AT_END;
    }

    @Override
    public SqlNodeVisitor<String> getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new PostgresSQLSqlGenerationVisitor(this, context);
    }

    @Override
    public void validateProperties() throws PropertyValidationException {
        super.validateProperties();
        checkPostgreSQLIdentifierPropertyConsistency();
    }

    private void checkPostgreSQLIdentifierPropertyConsistency() throws PropertyValidationException {
        if (this.properties.containsKey(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY)) {
            final String propertyValue = this.properties.get(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY);
            if (!propertyValue.equals(POSTGRESQL_IDENTIFER_MAPPING_PRESERVE_ORIGINAL_CASE_VALUE)
                    && !propertyValue.equals(POSTGRESQL_IDENTIFIER_MAPPING_CONVERT_TO_UPPER_VALUE)) {
                throw new PropertyValidationException(ExaError.messageBuilder("E-PGVS-4")
                        .message("Value for " + POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY + " must be "
                                + POSTGRESQL_IDENTIFER_MAPPING_PRESERVE_ORIGINAL_CASE_VALUE + " or "
                                + POSTGRESQL_IDENTIFIER_MAPPING_CONVERT_TO_UPPER_VALUE)
                        .toString());
            }
        }
        if (this.properties.hasIgnoreErrors()
                && !List.of(POSTGRESQL_UPPERCASE_TABLES_SWITCH).containsAll(this.properties.getIgnoredErrors())) {
            throw new PropertyValidationException(ExaError
                    .messageBuilder("E-PGVS-5").message("Unknown error identifier in list of ignored errors ("
                            + IGNORE_ERRORS_PROPERTY + "). Pick one of: " + POSTGRESQL_UPPERCASE_TABLES_SWITCH)
                    .toString());
        }
    }

    @Override
    // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS
    public String getStringLiteral(final String value) {
        if (value == null) {
            return "NULL";
        }
        // We use an escape string constant to be independent of the parameter standard_conforming_strings.
        // We use '' instead of \' to be independent of the parameter backslash_quote.
        return "E'" + value.replace("\\", "\\\\").replace("'", "''") + "'";
    }
}