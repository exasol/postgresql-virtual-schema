package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.*;
import java.sql.Date;
import java.util.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.adapter.dialects.scalarfunction.ScalarFunctionsAbstractIT;
import com.exasol.adapter.dialects.scalarfunction.StringTestTable;
import com.exasol.closeafterall.CloseAfterAll;
import com.exasol.closeafterall.CloseAfterAllExtension;
import com.exasol.dbbuilder.dialects.Schema;
import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

@ExtendWith({ CloseAfterAllExtension.class })
class PostgreSQLScalarFunctionsIT extends ScalarFunctionsAbstractIT {
    private static final String POSTGRES_TABLE_NAME = "my_postgres_table";
    /**
     * These functions are known to be broken in this dialect. We will remove them once they are fixed.
     */
    private static final Set<ScalarFunctionCapability> KNOWN_BROKEN = Set.of(GREATEST, LEAST, ROUND, CONCAT, INSTR,
            UNICODECHR, UNICODE, ADD_DAYS, ADD_HOURS, ADD_MINUTES, ADD_MONTHS, HOURS_BETWEEN, ADD_SECONDS, ADD_WEEKS,
            ADD_YEARS, SECONDS_BETWEEN, MINUTES_BETWEEN, SECOND, TO_CHAR, POSIX_TIME);
    @CloseAfterAll
    private static final PostgresVirtualSchemaIntegrationTestSetup SETUP = new PostgresVirtualSchemaIntegrationTestSetup();
    private static Schema postgresSchema;
    private static VirtualSchema virtualSchema;
    private final List<Table> createdTables = new LinkedList<>();

    @Override
    protected Set<ScalarFunctionCapability> getDialectSpecificExcludes() {
        return KNOWN_BROKEN;
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
    protected String createDateVirtualSchemaTableWithSoleValue(final Timestamp soleValue) throws SQLException {
        final Table table = postgresSchema.createTableBuilder(POSTGRES_TABLE_NAME)//
                .column("timestamp", "TIMESTAMP").build()//
                .insert(soleValue);
        this.createdTables.add(table);
        refreshVirtualSchema();
        return virtualSchema.getFullyQualifiedName() + "." + POSTGRES_TABLE_NAME;
    }

    @Override
    protected String createIntegerVirtualSchemaTableWithSoleRowValue(final int soleRowValue) throws SQLException {
        final Table table = postgresSchema.createTableBuilder(POSTGRES_TABLE_NAME)//
                .column("number", "integer").build()//
                .insert(soleRowValue);
        this.createdTables.add(table);
        refreshVirtualSchema();
        return virtualSchema.getFullyQualifiedName() + "." + POSTGRES_TABLE_NAME;
    }

    @Override
    protected String createBooleanVirtualSchemaTableWithSoleTrueValue() throws SQLException {
        final Table table = postgresSchema.createTableBuilder(POSTGRES_TABLE_NAME)//
                .column("boolean", "boolean").build()//
                .insert(true);
        this.createdTables.add(table);
        refreshVirtualSchema();
        return virtualSchema.getFullyQualifiedName() + "." + POSTGRES_TABLE_NAME;
    }

    @Override
    protected StringTestTable createStringVirtualSchemaTable() throws SQLException {
        final Table table = postgresSchema.createTableBuilder(POSTGRES_TABLE_NAME)//
                .column("string", "VARCHAR(500)").build()//
                .insert("");
        this.createdTables.add(table);
        refreshVirtualSchema();
        final String tableName = virtualSchema.getFullyQualifiedName() + "." + POSTGRES_TABLE_NAME;
        final StringTestTable.ValueSetter valueSetter = (value) -> {
            truncateTable(table);
            table.insert(value);
        };
        return new StringTestTable(tableName, valueSetter);
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
