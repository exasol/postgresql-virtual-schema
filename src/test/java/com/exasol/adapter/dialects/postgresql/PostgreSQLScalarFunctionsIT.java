package com.exasol.adapter.dialects.postgresql;

import java.sql.*;
import java.sql.Date;
import java.util.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.adapter.dialects.scalarfunction.ScalarFunctionsAbstractIT;
import com.exasol.adapter.dialects.scalarfunction.VirtualSchemaTestTable;
import com.exasol.closeafterall.CloseAfterAll;
import com.exasol.closeafterall.CloseAfterAllExtension;
import com.exasol.dbbuilder.dialects.Schema;
import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

@ExtendWith({ CloseAfterAllExtension.class })
class PostgreSQLScalarFunctionsIT extends ScalarFunctionsAbstractIT {
    private static final String POSTGRES_TABLE_NAME = "my_postgres_table";
    @CloseAfterAll
    private static final PostgresVirtualSchemaIntegrationTestSetup SETUP = new PostgresVirtualSchemaIntegrationTestSetup();
    private static Schema postgresSchema;
    private static VirtualSchema virtualSchema;
    private final List<Table> createdTables = new LinkedList<>();

    @Override
    protected Set<ScalarFunctionCapability> getDialectSpecificExcludes() {
        return Set.of();
    }

    @BeforeAll
    static void beforeAll() {
        postgresSchema = SETUP.getPostgresFactory().createSchema("schema_postgres");
        virtualSchema = SETUP.createVirtualSchema(postgresSchema.getName(), Map.of());
    }

    private void refreshVirtualSchema() throws SQLException {
        SETUP.getExasolStatement()
                .executeUpdate("ALTER VIRTUAL SCHEMA " + virtualSchema.getFullyQualifiedName() + " REFRESH");
    }

    @Override
    protected String createVirtualSchemaTableWithExamplesForAllDataTypes() throws SQLException {
        final Table table = postgresSchema.createTableBuilder(POSTGRES_TABLE_NAME)//
                .column("floating_point", "real")//
                .column("number", "integer")//
                .column("boolean", "boolean")//
                .column("string", "VARCHAR(2)")//
                .column("date", "DATE")//
                .column("timestamp", "TIMESTAMP").build()
                .insert(0.5, 2, true, "a", new Date(1000), new Timestamp(1001));
        this.createdTables.add(table);
        refreshVirtualSchema();
        return virtualSchema.getFullyQualifiedName() + "." + POSTGRES_TABLE_NAME;
    }

    @Override
    protected String createAnyVirtualSchemaTable() throws SQLException {
        return createVirtualSchemaTableWithExamplesForAllDataTypes();
    }

    @Override
    protected String createDateVirtualSchemaTableWithSingleValue(final Timestamp soleValue) throws SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        final Table table = postgresSchema.createTableBuilder(POSTGRES_TABLE_NAME)//
                .column("timestamp", "TIMESTAMP").build()//
                .insert(soleValue);
        this.createdTables.add(table);
        refreshVirtualSchema();
        return virtualSchema.getFullyQualifiedName() + "." + POSTGRES_TABLE_NAME;
    }

    @Override
    protected VirtualSchemaTestTable<Integer> createIntegerVirtualSchemaTable() throws SQLException {
        return createTestTable("integer");
    }

    @Override
    protected VirtualSchemaTestTable<Double> createDoubleVirtualSchemaTable() throws SQLException {
        return createTestTable("real");
    }

    private <T> VirtualSchemaTestTable<T> createTestTable(final String type) throws SQLException {
        final Table table = postgresSchema.createTableBuilder(POSTGRES_TABLE_NAME)//
                .column("my_column", type).build();
        this.createdTables.add(table);
        refreshVirtualSchema();
        final String tableName = virtualSchema.getFullyQualifiedName() + "." + POSTGRES_TABLE_NAME;
        return new VirtualSchemaTestTable<>(tableName, getTruncateValueSetter(table));
    }

    private <T> VirtualSchemaTestTable.SingleRowTableProvisioner<T> getTruncateValueSetter(final Table table) {
        return (value) -> {
            truncateTable(table);
            table.insert(value);
        };
    }

    @Override
    protected String createBooleanVirtualSchemaTableWithSingleTrueValue() throws SQLException {
        final Table table = postgresSchema.createTableBuilder(POSTGRES_TABLE_NAME)//
                .column("boolean", "boolean").build()//
                .insert(true);
        this.createdTables.add(table);
        refreshVirtualSchema();
        return virtualSchema.getFullyQualifiedName() + "." + POSTGRES_TABLE_NAME;
    }

    @Override
    protected VirtualSchemaTestTable<String> createStringVirtualSchemaTable() throws SQLException {
        return createTestTable("VARCHAR(500)");
    }

    private void truncateTable(final Table table) throws SQLException {
        SETUP.getPostgresqlStatement().executeUpdate("TRUNCATE TABLE " + table.getFullyQualifiedName());
    }

    @Override
    protected void deleteCreatedTables() {
        this.createdTables.forEach(Table::drop);
        this.createdTables.clear();
    }

    protected Connection createExasolConnection() throws SQLException {
        return SETUP.getExasolContainer().createConnection();
    }
}
