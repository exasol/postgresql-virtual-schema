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
    protected SingleTableVirtualSchemaTestSetup createVirtualSchemaTableWithExamplesForAllDataTypes() {
        return new PostgreSQLSingleTableVirtualSchemaTestSetup() {
            @Override
            protected Table createTable() {
                return this.getPostgresqlSchema().createTableBuilder(getUniqueIdentifier())//
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
    protected SingleRowSingleTableVirtualSchemaTestSetup<Timestamp> createDateVirtualSchemaTable() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        return createTestTable("timestamp");
    }

    @Override
    protected SingleRowSingleTableVirtualSchemaTestSetup<Integer> createIntegerVirtualSchemaTable() {
        return createTestTable("integer");
    }

    @Override
    protected SingleRowSingleTableVirtualSchemaTestSetup<Double> createDoubleVirtualSchemaTable() {
        return createTestTable("real");
    }

    @Override
    protected SingleRowSingleTableVirtualSchemaTestSetup<Boolean> createBooleanVirtualSchemaTable() {
        return createTestTable("boolean");
    }

    private <T> SingleRowSingleTableVirtualSchemaTestSetup<T> createTestTable(final String type) {
        return new SingleRowPostgreSQLSingleTableVirtualSchemaTestSetup<>() {
            @Override
            protected Table createTable() {
                return this.getPostgresqlSchema().createTableBuilder(getUniqueIdentifier())//
                        .column("my_column", type).build();
            }
        };
    }

    @Override
    protected Connection createExasolConnection() throws SQLException {
        return SETUP.getExasolContainer().createConnection();
    }

    @Override
    protected SingleRowSingleTableVirtualSchemaTestSetup<String> createStringVirtualSchemaTable() {
        return createTestTable("VARCHAR(500)");
    }

    private static abstract class PostgreSQLSingleTableVirtualSchemaTestSetup
            implements SingleTableVirtualSchemaTestSetup {
        private final VirtualSchema virtualSchema;
        private final Table table;
        private final Schema postgresqlSchema;

        public PostgreSQLSingleTableVirtualSchemaTestSetup() {
            this.postgresqlSchema = SETUP.getPostgresFactory().createSchema(getUniqueIdentifier());
            this.table = createTable();
            this.virtualSchema = SETUP.createVirtualSchema(this.postgresqlSchema.getName(), Map.of());
        }

        protected abstract Table createTable();

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

        public Schema getPostgresqlSchema() {
            return this.postgresqlSchema;
        }

        public Table getTable() {
            return this.table;
        }
    }

    private static abstract class SingleRowPostgreSQLSingleTableVirtualSchemaTestSetup<T> extends
            PostgreSQLSingleTableVirtualSchemaTestSetup implements SingleRowSingleTableVirtualSchemaTestSetup<T> {

        @Override
        public void truncateTable() throws SQLException {
            SETUP.getPostgresqlStatement().executeUpdate("TRUNCATE TABLE " + this.getTable().getFullyQualifiedName());
        }

        @Override
        public void insertValue(final T value) {
            getTable().insert(value);
        }
    }
}
