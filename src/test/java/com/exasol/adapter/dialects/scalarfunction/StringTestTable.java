package com.exasol.adapter.dialects.scalarfunction;

import java.sql.SQLException;

public class StringTestTable {
    private final String fullyQualifiedName;
    private final ValueSetter valueSetter;

    /**
     * Create a new instance of {@link StringTestTable}.
     *
     * @param fullyQualifiedName name of the table in the virtual schema
     * @param valueSetter        function that can change the value of the sole row of the table
     */
    public StringTestTable(final String fullyQualifiedName, final ValueSetter valueSetter) {
        this.fullyQualifiedName = fullyQualifiedName;
        this.valueSetter = valueSetter;
    }

    public String getFullyQualifiedName() {
        return this.fullyQualifiedName;
    }

    public ValueSetter getValueSetter() {
        return this.valueSetter;
    }

    @FunctionalInterface
    public interface ValueSetter {
        void setValueOfSoleRow(String value) throws SQLException;
    }
}
