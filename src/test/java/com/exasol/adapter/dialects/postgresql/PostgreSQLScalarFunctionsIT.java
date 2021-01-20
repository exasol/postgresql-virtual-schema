package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.*;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.extension.ExtendWith;

import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.adapter.dialects.ScalarFunctionsAbstractIT;
import com.exasol.closeafterall.CloseAfterAll;
import com.exasol.closeafterall.CloseAfterAllExtension;
import com.exasol.dbbuilder.dialects.Schema;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

@ExtendWith({ CloseAfterAllExtension.class })
class PostgreSQLScalarFunctionsIT extends ScalarFunctionsAbstractIT {
    private static final String EMPTY_POSTGRES_TABLE = "empty_postgres_table";
    /**
     * These functions are known to be broken in this dialect. We will remove them once they are fixed.
     */
    private static final Set<ScalarFunctionCapability> KNOWN_BROKEN = Set.of(GREATEST, LEAST, ROUND, CONCAT, INSTR,
            UNICODECHR, UNICODE, ADD_DAYS, ADD_HOURS, ADD_MINUTES, ADD_MONTHS, HOURS_BETWEEN, ADD_SECONDS, ADD_WEEKS,
            ADD_YEARS, SECONDS_BETWEEN, MINUTES_BETWEEN, SECOND, TO_CHAR);
    @CloseAfterAll
    private final PostgresVirtualSchemaIntegrationTestSetup setup = new PostgresVirtualSchemaIntegrationTestSetup();

    @Override
    protected Set<ScalarFunctionCapability> getDialectSpecificExcludes() {
        return KNOWN_BROKEN;
    }

    @Override
    protected String setupDatabase() throws SQLException {
        final Schema postgresSchema = this.setup.getPostgresFactory().createSchema("schema_postgres");
        postgresSchema.createTableBuilder(EMPTY_POSTGRES_TABLE)//
                .column("floating_point", "real")//
                .column("number", "integer")//
                .column("zero", "integer")//
                .column("boolean", "boolean")//
                .column("string", "VARCHAR(2)")//
                .column("empty_string", "VARCHAR(2)")//
                .column("date", "DATE")//
                .column("timestamp", "TIMESTAMP").build()
                .insert(0.5, 2, 0, true, "a", "", new Date(1000), new Timestamp(1001));
        final VirtualSchema virtualSchema = this.setup.createVirtualSchema(postgresSchema.getName(), Map.of());
        return virtualSchema.getFullyQualifiedName() + "." + EMPTY_POSTGRES_TABLE;
    }

    protected Connection createExasolConnection() throws SQLException {
        return this.setup.getExasolContainer().createConnection();
    }
}
