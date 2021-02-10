package com.exasol.adapter.dialects.scalarfunction;

import java.sql.SQLException;

public class VirtualSchemaTestTable<T> {
    private final String fullyQualifiedName;
    private final ValueSetter<T> valueSetter;

    /**
     * Create a new instance of {@link VirtualSchemaTestTable}.
     *
     * @param fullyQualifiedName name of the table in the virtual schema
     * @param valueSetter        function that can change the value of the single row of the table
     */
    public VirtualSchemaTestTable(final String fullyQualifiedName, final ValueSetter<T> valueSetter) {
        this.fullyQualifiedName = fullyQualifiedName;
        this.valueSetter = valueSetter;
    }

    public String getFullyQualifiedName() {
        return this.fullyQualifiedName;
    }

    public ValueSetter<T> getValueSetter() {
        return this.valueSetter;
    }

    @FunctionalInterface
    public interface ValueSetter<T> {
        void setValueOfSingleRow(T value) throws SQLException;
    }
}
