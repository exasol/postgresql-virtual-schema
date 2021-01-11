package com.exasol.adapter.dialects.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
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

    @Override
    protected String setupDatabase() throws SQLException {
        final Schema postgresSchema = this.setup.getPostgresFactory().createSchema("schema_postgres");
        postgresSchema.createTable(EMPTY_POSTGRES_TABLE, "id", "integer").insert(1);
        final VirtualSchema virtualSchema = this.setup.createVirtualSchema(postgresSchema.getName(), Map.of());
        return virtualSchema.getFullyQualifiedName() + "." + EMPTY_POSTGRES_TABLE;
    }

    protected Connection createExasolConnection() throws SQLException {
        return this.setup.getExasolContainer().createConnection();
    }
}
