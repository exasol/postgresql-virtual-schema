package com.exasol.adapter.dialects.postgresql;

import java.sql.*;
import java.util.Map;

import org.junit.jupiter.api.extension.ExtendWith;

import com.exasol.adapter.dialects.ScalarFunctionsAbstractIT;
import com.exasol.closeafterall.CloseAfterAll;
import com.exasol.closeafterall.CloseAfterAllExtension;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

@ExtendWith({ CloseAfterAllExtension.class })
class PostgreSQLScalarFunctionsIT extends ScalarFunctionsAbstractIT {

    private static final String SCHEMA_POSTGRES = "schema_postgres";
    private static final String EMPTY_POSTGRES_TABLE = "empty_postgres_table";
    @CloseAfterAll
    private final PostgresVirtualSchemaIntegrationTestSetup setup = new PostgresVirtualSchemaIntegrationTestSetup();

    @Override
    protected Setup setupDatabase() {
        try {
            final Statement postgresqlStatement = this.setup.getPostgresqlStatement();
            postgresqlStatement.executeUpdate("CREATE SCHEMA " + SCHEMA_POSTGRES);
            postgresqlStatement
                    .executeUpdate("CREATE TABLE " + SCHEMA_POSTGRES + "." + EMPTY_POSTGRES_TABLE + "(id integer)");
            postgresqlStatement
                    .executeUpdate("INSERT INTO " + SCHEMA_POSTGRES + "." + EMPTY_POSTGRES_TABLE + " VALUES(1);");
            final VirtualSchema virtualSchema = this.setup.createVirtualSchema(SCHEMA_POSTGRES, Map.of());
            return new Setup(virtualSchema.getFullyQualifiedName() + "." + EMPTY_POSTGRES_TABLE,
                    this::createConnection);
        } catch (final SQLException exception) {
            throw new IllegalStateException("Failed to create test setup.", exception);
        }
    }

    private Connection createConnection() {
        try {
            return this.setup.getExasolContainer().createConnection();
        } catch (final SQLException exception) {
            throw new IllegalStateException("Failed to create Exasol statement.");
        }
    }
}
