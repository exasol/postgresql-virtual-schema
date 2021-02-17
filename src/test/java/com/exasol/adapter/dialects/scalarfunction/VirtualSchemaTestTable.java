package com.exasol.adapter.dialects.scalarfunction;

import java.sql.SQLException;

public class VirtualSchemaTestTable<T> {
    private final String fullyQualifiedName;
    private final SingleRowTableProvisioner<T> singleRowTableProvisioner;

    /**
     * Create a new instance of {@link VirtualSchemaTestTable}.
     *
     * @param fullyQualifiedName        name of the table in the virtual schema
     * @param singleRowTableProvisioner function that can change the value of the single row of the table
     */
    public VirtualSchemaTestTable(final String fullyQualifiedName,
            final SingleRowTableProvisioner<T> singleRowTableProvisioner) {
        this.fullyQualifiedName = fullyQualifiedName;
        this.singleRowTableProvisioner = singleRowTableProvisioner;
    }

    public String getFullyQualifiedName() {
        return this.fullyQualifiedName;
    }

    public void setValueOfSingleRow(final T value) throws SQLException {
        this.singleRowTableProvisioner.initializedSingleRow(value);
    }

    @FunctionalInterface
    public interface SingleRowTableProvisioner<T> {
        /**
         * Set the value of the single row of the table. Typical implementation: truncate table + insert row with the
         * given value.
         * 
         * @param value value for the row
         * @throws SQLException if something goes wrong
         */
        void initializedSingleRow(T value) throws SQLException;
    }
}
