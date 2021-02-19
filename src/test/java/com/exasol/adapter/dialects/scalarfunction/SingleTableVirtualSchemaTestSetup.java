package com.exasol.adapter.dialects.scalarfunction;

public interface SingleTableVirtualSchemaTestSetup {

    /**
     * Get the fully qualified name of the virtual schema table.
     * 
     * @return fully qualified name
     */
    public String getFullyQualifiedName();

    /**
     * Drop the virtual schema and the tables created for it.
     */
    public void drop();
}
