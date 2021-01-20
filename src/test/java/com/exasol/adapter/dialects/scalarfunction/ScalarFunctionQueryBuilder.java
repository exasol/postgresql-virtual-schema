package com.exasol.adapter.dialects.scalarfunction;

public class ScalarFunctionQueryBuilder {
    private final String tableName;

    public ScalarFunctionQueryBuilder(final String tableName) {
        this.tableName = tableName;
    }

    public String buildQueryFor(final String functionCall) {
        return "SELECT " + functionCall + " FROM " + this.tableName;
    }
}
