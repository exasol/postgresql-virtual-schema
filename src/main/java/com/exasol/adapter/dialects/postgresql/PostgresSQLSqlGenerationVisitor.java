package com.exasol.adapter.dialects.postgresql;

import java.util.*;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;
import com.exasol.errorreporting.ExaError;

/**
 * This class generates SQL queries for the {@link PostgreSQLSqlDialect}.
 */
public class PostgresSQLSqlGenerationVisitor extends SqlGenerationVisitor {
    private static final List<String> TYPE_NAMES_REQUIRING_CAST = List.of("varbit", "point", "line", "lseg", "box",
            "path", "polygon", "circle", "cidr", "citext", "inet", "macaddr", "interval", "json", "jsonb", "uuid",
            "tsquery", "tsvector", "xml", "smallserial", "serial", "bigserial");
    private static final List<String> TYPE_NAMES_NOT_SUPPORTED = List.of("bytea");

    /**
     * Create a new instance of the {@link PostgresSQLSqlGenerationVisitor}.
     *
     * @param dialect {@link PostgreSQLSqlDialect} SQL dialect
     * @param context SQL generation context
     */
    public PostgresSQLSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }

    protected List<String> getListOfTypeNamesRequiringCast() {
        return TYPE_NAMES_REQUIRING_CAST;
    }

    protected List<String> getListOfTypeNamesNotSupported() {
        return TYPE_NAMES_NOT_SUPPORTED;
    }

    @Override
    protected String representAnyColumnInSelectList() {
        return SqlConstants.ONE;
    }

    @Override
    protected String representAsteriskInSelectList(final SqlSelectList selectList) throws AdapterException {
        final List<String> selectStarList = buildSelectStar(selectList);
        final List<String> selectListElements = new ArrayList<>(selectStarList.size());
        selectListElements.addAll(selectStarList);
        return String.join(", ", selectListElements);
    }

    private List<String> buildSelectStar(final SqlSelectList selectList) throws AdapterException {
        if (SqlGenerationHelper.selectListRequiresCasts(selectList, this.nodeRequiresCast)) {
            return buildSelectStarWithNodeCast(selectList);
        } else {
            return new ArrayList<>(Collections.singletonList("*"));
        }
    }

    private List<String> buildSelectStarWithNodeCast(final SqlSelectList selectList) throws AdapterException {
        final SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
        int columnId = 0;
        final List<TableMetadata> tableMetadata = new ArrayList<>();
        SqlGenerationHelper.addMetadata(select.getFromClause(), tableMetadata);
        final List<String> selectListElements = new ArrayList<>(tableMetadata.size());
        for (final TableMetadata tableMeta : tableMetadata) {
            for (final ColumnMetadata columnMeta : tableMeta.getColumns()) {
                final SqlColumn sqlColumn = new SqlColumn(columnId, columnMeta);
                selectListElements.add(buildColumnProjectionString(sqlColumn, super.visit(sqlColumn)));
                ++columnId;
            }
        }
        return selectListElements;
    }

    private String buildColumnProjectionString(final SqlColumn column, final String projectionString)
            throws AdapterException {
        return buildColumnProjectionString(getTypeNameFromColumn(column), projectionString);
    }

    private final java.util.function.Predicate<SqlNode> nodeRequiresCast = node -> {
        try {
            if (node.getType() == SqlNodeType.COLUMN) {
                final SqlColumn column = (SqlColumn) node;
                final String typeName = getTypeNameFromColumn(column);
                return getListOfTypeNamesRequiringCast().contains(typeName)
                        || getListOfTypeNamesNotSupported().contains(typeName);
            }
            return false;
        } catch (final AdapterException exception) {
            throw new SqlGenerationVisitorException(ExaError.messageBuilder("E-PGVS-7")
                    .message("Exception during deserialization of ColumnAdapterNotes.").toString(), exception);
        }
    };

    @Override
    public String visit(final SqlColumn column) throws AdapterException {
        final String projectionString = super.visit(column);
        return getColumnProjectionString(column, projectionString);
    }

    private String getColumnProjectionString(final SqlColumn column, final String projectionString)
            throws AdapterException {
        return super.isDirectlyInSelectList(column) //
                ? buildColumnProjectionString(getTypeNameFromColumn(column), projectionString) //
                : projectionString;
    }

    @Override
    public String visit(final SqlFunctionScalar function) throws AdapterException {
        final List<SqlNode> arguments = function.getArguments();
        final List<String> argumentsSql = new ArrayList<>(arguments.size());
        for (final SqlNode node : arguments) {
            argumentsSql.add(node.accept(this));
        }
        final ScalarFunction scalarFunction = function.getFunction();
        switch (scalarFunction) {
        case ADD_DAYS:
            return getAddDateTime(argumentsSql, "days");
        case ADD_HOURS:
            return getAddDateTime(argumentsSql, "hours");
        case ADD_MINUTES:
            return getAddDateTime(argumentsSql, "mins");
        case ADD_SECONDS:
            return getAddDateTime(argumentsSql, "secs");
        case ADD_WEEKS:
            return getAddDateTime(argumentsSql, "weeks");
        case ADD_YEARS:
            return getAddDateTime(argumentsSql, "years");
        case ADD_MONTHS:
            return getAddDateTime(argumentsSql, "months");
        case SECOND:
        case MINUTE:
        case DAY:
        case WEEK:
        case MONTH:
        case YEAR:
            return getDateTime(argumentsSql, scalarFunction);
        case POSIX_TIME:
            return getPosixTime(argumentsSql);
        default:
            return super.visit(function);
        }
    }

    private String getAddDateTime(final List<String> argumentsSql, final String unit) {
        final StringBuilder builder = new StringBuilder();
        builder.append(argumentsSql.get(0));
        builder.append(" + ");
        builder.append(buildInterval(argumentsSql, unit));
        return builder.toString();
    }

    private String buildInterval(final List<String> argumentsSql, final String unit) {
        return "make_interval(" + unit + " => " + argumentsSql.get(1) + ")";
    }

    private String getDateTime(final List<String> argumentsSql, final ScalarFunction scalarFunction) {
        final StringBuilder builder = new StringBuilder();
        builder.append("DATE_PART(");
        switch (scalarFunction) {
        case SECOND:
            builder.append("'SECOND'");
            break;
        case MINUTE:
            builder.append("'MINUTE'");
            break;
        case DAY:
            builder.append("'DAY'");
            break;
        case WEEK:
            builder.append("'WEEK'");
            break;
        case MONTH:
            builder.append("'MONTH'");
            break;
        case YEAR:
            builder.append("'YEAR'");
            break;
        default:
            break;
        }
        builder.append(",");
        builder.append(argumentsSql.get(0));
        builder.append(")");
        return builder.toString();
    }

    private String getPosixTime(final List<String> argumentsSql) {
        return "EXTRACT(EPOCH FROM " + argumentsSql.get(0) + ")";
    }

    private String buildColumnProjectionString(final String typeName, final String projectionString) {
        if (checkIfNeedToCastToVarchar(typeName)) {
            return "CAST(" + projectionString + "  as VARCHAR )";
        } else if (typeName.startsWith("smallserial")) {
            return "CAST(" + projectionString + "  as SMALLINT )";
        } else if (typeName.startsWith("serial")) {
            return "CAST(" + projectionString + "  as INTEGER )";
        } else if (typeName.startsWith("bigserial")) {
            return "CAST(" + projectionString + "  as BIGINT )";
        } else if (TYPE_NAMES_NOT_SUPPORTED.contains(typeName)) {
            return "cast('" + typeName + " NOT SUPPORTED' as varchar) as not_supported";
        } else {
            return projectionString;
        }
    }

    private boolean checkIfNeedToCastToVarchar(final String typeName) {
        final List<String> typesToVarcharCast = Arrays.asList("point", "line", "varbit", "lseg", "box", "path",
                "polygon", "circle", "cidr", "citext", "inet", "macaddr", "interval", "json", "jsonb", "uuid",
                "tsquery", "tsvector", "xml");
        return typesToVarcharCast.contains(typeName);
    }

    @Override
    public String visit(final SqlFunctionAggregateGroupConcat function) throws AdapterException {
        final StringBuilder builder = new StringBuilder();
        builder.append("STRING_AGG");
        builder.append("(");
        final String expression = function.getArgument().accept(this);
        builder.append(expression);
        builder.append(", ");
        final String separator = function.hasSeparator() ? function.getSeparator().accept(this) : "','";
        builder.append(separator);
        builder.append(") ");
        return builder.toString();
    }
}