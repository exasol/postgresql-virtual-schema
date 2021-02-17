package com.exasol.adapter.dialects.postgresql;

import java.sql.*;
import java.sql.Date;
import java.time.Instant;
import java.util.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.adapter.dialects.scalarfunction.*;
import com.exasol.closeafterall.CloseAfterAll;
import com.exasol.closeafterall.CloseAfterAllExtension;
import com.exasol.dbbuilder.dialects.Schema;
import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

@ExtendWith({ CloseAfterAllExtension.class })
class PostgreSQLScalarFunctionsIT extends ScalarFunctionsAbstractIT {
    @CloseAfterAll
    private static final PostgresVirtualSchemaIntegrationTestSetup SETUP = new PostgresVirtualSchemaIntegrationTestSetup();

    @Override
    protected Set<ScalarFunctionCapability> getDialectSpecificExcludes() {
        return Collections.emptySet();
    }

    @BeforeAll
    static void beforeAll() {
    }

    private static String getUniqueIdentifier() {
        final Instant now = Instant.now();
        final int randomPart = (int) (Math.random() * 1000);
        return "id" + now.getEpochSecond() + now.getNano() + randomPart;
    }

    @Override
    protected VirtualSchemaTestTable createVirtualSchemaTableWithExamplesForAllDataTypes() {
        return new PostgresVirtualSchemaTestTable() {
            @Override
            protected Table createTable(final Schema schema) {
                return schema.createTableBuilder(getUniqueIdentifier())//
                        .column("floating_point", "real")//
                        .column("number", "integer")//
                        .column("boolean", "boolean")//
                        .column("string", "VARCHAR(2)")//
                        .column("date", "DATE")//
                        .column("timestamp", "TIMESTAMP").build()
                        .insert(0.5, 2, true, "a", new Date(1000), new Timestamp(1001));
            }
        };
    }

    @Override
    protected SingleValueVirtualSchemaTestTable<Timestamp> createDateVirtualSchemaTable() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        return createTestTable("timestamp");
    }

    @Override
    protected SingleValueVirtualSchemaTestTable<Integer> createIntegerVirtualSchemaTable() {
        return createTestTable("integer");
    }

    @Override
    protected SingleValueVirtualSchemaTestTable<Double> createDoubleVirtualSchemaTable() {
        return createTestTable("real");
    }

    @Override
    protected SingleValueVirtualSchemaTestTable<Boolean> createBooleanVirtualSchemaTable() {
        return createTestTable("boolean");
    }

    private <T> SingleValueVirtualSchemaTestTable<T> createTestTable(final String type) {
        return new SingleValuePostgresVirtualSchemaTestTable<>() {
            @Override
            protected Table createTable(final Schema schema) {
                return schema.createTableBuilder(getUniqueIdentifier())//
                        .column("my_column", type).build();
            }
        };
    }

    protected Connection createExasolConnection() throws SQLException {
        return SETUP.getExasolContainer().createConnection();
    }

    @Override
    protected SingleValueVirtualSchemaTestTable<String> createStringVirtualSchemaTable() {
        return createTestTable("VARCHAR(500)");
    }

    private static abstract class PostgresVirtualSchemaTestTable implements VirtualSchemaTestTable {
        private final VirtualSchema virtualSchema;
        private final Table table;
        private final Schema postgresqlSchema;

        public PostgresVirtualSchemaTestTable() {
            this.postgresqlSchema = SETUP.getPostgresFactory().createSchema(getUniqueIdentifier());
            this.table = createTable(this.postgresqlSchema);
            this.virtualSchema = SETUP.createVirtualSchema(this.postgresqlSchema.getName(), Map.of());
        }

        protected abstract Table createTable(Schema schema);

        @Override
        public String getFullyQualifiedName() {
            return this.virtualSchema.getFullyQualifiedName() + "." + this.table.getName();
        }

        @Override
        public void drop() {
            this.virtualSchema.drop();
            this.table.drop();
            this.postgresqlSchema.drop();
        }

        public Table getTable() {
            return this.table;
        }
    }

    private static abstract class SingleValuePostgresVirtualSchemaTestTable<T> extends PostgresVirtualSchemaTestTable
            implements SingleValueVirtualSchemaTestTable<T> {
        @Override
        public void initializeSingleRow(final T value) throws SQLException {
            truncateTable(this.getTable());
            getTable().insert(value);
        }

        private void truncateTable(final Table table) throws SQLException {
            SETUP.getPostgresqlStatement().executeUpdate("TRUNCATE TABLE " + table.getFullyQualifiedName());
        }
    }
}
