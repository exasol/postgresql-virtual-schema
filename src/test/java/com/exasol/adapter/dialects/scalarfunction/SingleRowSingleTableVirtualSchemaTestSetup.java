package com.exasol.adapter.dialects.scalarfunction;

import java.sql.SQLException;

/**
 * This is an interface for test setups that containing a virtual schema table. The test setup allows users of this
 * interface to change the value of the single row of the table in the virtual schema. Sin ce this is not possible
 * (Virtual Schemas don't store values but act like views), the test setup has to change the value in a table that the
 * virtual schema reflects.
 * 
 * @param <T> type of the value to set
 */
public interface SingleRowSingleTableVirtualSchemaTestSetup<T> extends SingleTableVirtualSchemaTestSetup {

    /**
     * Set the value of the single row of the table.
     *
     * @param value value to set
     * @throws SQLException if something goes wrong
     */
    public default void initializeSingleRowWith(final T value) throws SQLException {
        truncateTable();
        insertValue(value);
    }

    /**
     * Delete all rows of the table.
     * 
     * @throws SQLException if something goes wrong
     */
    public void truncateTable() throws SQLException;

    /**
     * Insert a row into the table.
     *
     * @param value value to insert
     * @throws SQLException if something goes wrong
     */
    public void insertValue(T value) throws SQLException;
}
