package com.exasol.adapter.dialects.postgresql.docgeneration;

import java.nio.file.Path;
import java.util.Set;

import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectFactory;
import com.exasol.autogeneratedresourceverifier.AutogeneratedResource;

import net.steppschuh.markdowngenerator.table.Table;

public class CapabilitiesReport implements AutogeneratedResource {
    private final SqlDialectFactory dialectFactory;

    public CapabilitiesReport(final SqlDialectFactory dialectFactory) {
        this.dialectFactory = dialectFactory;
    }

    @Override
    public String generateContent() {
        final LinedStringBuilder reportBuilder = new LinedStringBuilder();
        final SqlDialect sqlDialect = this.dialectFactory.createSqlDialect(null, null);
        final Capabilities capabilities = sqlDialect.getCapabilities();
        reportBuilder.appendLine(
                "<!-- DON'T CHANGE THIS FILE! It's autogenerated from " + this.getClass().getName() + " --> ");
        reportBuilder.appendLine();
        reportBuilder.appendLine("# Capabilities");
        reportBuilder.appendLine();
        reportBuilder.appendLine(
                "Capabilities tell the Exasol which SQL features / keywords a Virtual Schema adapter supports. "
                        + "If the Virtual Schema does not support a certain capability, Exasol rewrites the query without that feature. "
                        + "In case a Virtual Schema adapter has no capabilities at all, Exasol will rewrite all queries to `SELECT * FROM table`. "
                        + "That means, that it will always load the whole remote table, even if only a single row is requested."
                        + "So, for optimizing your performance, make sure that at least all functions that you use in the `WHERE` clause of your queries are supported by the Virtual Schema adapter.");
        reportBuilder.appendLine();
        reportCapabilities("Main Capabilities", "Capability", MainCapability.values(),
                capabilities.getMainCapabilities(), reportBuilder);
        reportCapabilities("Supported Literals", "Literal", LiteralCapability.values(),
                capabilities.getLiteralCapabilities(), reportBuilder);
        reportCapabilities("Supported Predicates", "Predicate", PredicateCapability.values(),
                capabilities.getPredicateCapabilities(), reportBuilder);
        reportCapabilities("Supported Aggregate Functions", "Aggregate Function", AggregateFunctionCapability.values(),
                capabilities.getAggregateFunctionCapabilities(), reportBuilder);
        reportCapabilities("Supported Scalar Functions", "Scalar Function", ScalarFunctionCapability.values(),
                capabilities.getScalarFunctionCapabilities(), reportBuilder);
        return reportBuilder.toString();
    }

    @Override
    public Path getPathOfGeneratedFile() {
        return Path.of("doc", "generated", "capabilities.md");
    }

    private <T> void reportCapabilities(final String headline, final String tableHeader, final T[] all,
            final Set<T> enabled, final LinedStringBuilder reportBuilder) {
        reportBuilder.appendLine("## " + headline);
        reportBuilder.appendLine();
        final Table.Builder tableBuilder = new Table.Builder().withAlignments(Table.ALIGN_LEFT, Table.ALIGN_CENTER)
                .addRow(tableHeader, "Supported");
        for (final T function : all) {
            tableBuilder.addRow(function, getEnabledText(enabled.contains(function)));
        }
        reportBuilder.appendLine(tableBuilder.build().toString());
        reportBuilder.appendLine();
    }

    private String getEnabledText(final boolean enabled) {
        if (enabled) {
            return "✓";
        } else {
            return "";
        }
    }

    private static class LinedStringBuilder {
        private static final String LINE_SEPARATOR = System.lineSeparator();
        StringBuilder stringBuilder = new StringBuilder();

        public LinedStringBuilder appendLine(final String line) {
            this.stringBuilder.append(line).append(LINE_SEPARATOR);
            return this;
        }

        public LinedStringBuilder appendLine() {
            this.stringBuilder.append(LINE_SEPARATOR);
            return this;
        }

        @Override
        public String toString() {
            return this.stringBuilder.toString();
        }
    }
}