package com.exasol.adapter.dialects.postgresql;

import java.sql.*;
import java.util.Map;

import org.junit.jupiter.api.extension.ExtendWith;

import com.exasol.adapter.dialects.ScalarFunctionsAbstractIT;
import com.exasol.closeafterall.CloseAfterAll;
import com.exasol.closeafterall.CloseAfterAllExtension;
import com.exasol.dbbuilder.dialects.Schema;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

@ExtendWith({ CloseAfterAllExtension.class })
class PostgreSQLScalarFunctionsIT extends ScalarFunctionsAbstractIT {
    private static final String EMPTY_POSTGRES_TABLE = "empty_postgres_table";
    @CloseAfterAll
    private final PostgresVirtualSchemaIntegrationTestSetup setup = new PostgresVirtualSchemaIntegrationTestSetup();

    // TODO capital letters cause crash; bug?
    @Override
    protected String setupDatabase() throws SQLException {
        final Schema postgresSchema = this.setup.getPostgresFactory().createSchema("schema_postgres");
        postgresSchema.createTableBuilder(EMPTY_POSTGRES_TABLE)//
                .column("floating_point", "real")//
                .column("number", "integer")//
                .column("boolean", "boolean")//
                .column("string", "VARCHAR(2)")//
                .column("date", "DATE")//
                .column("timestamp", "TIMESTAMP").build()
                .insert(0.5, 2, true, "a", new Date(1000), new Timestamp(1001));
        final VirtualSchema virtualSchema = this.setup.createVirtualSchema(postgresSchema.getName(), Map.of());
        return virtualSchema.getFullyQualifiedName() + "." + EMPTY_POSTGRES_TABLE;
    }

    protected Connection createExasolConnection() throws SQLException {
        return this.setup.getExasolContainer().createConnection();
    }
}
