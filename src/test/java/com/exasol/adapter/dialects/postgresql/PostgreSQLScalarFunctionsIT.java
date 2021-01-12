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

    /*
     * private static final List<String> LITERALS_OLD = List.of("0.5", "2", "TRUE", "'a'", "DATE '2007-03-31'",
     * "TIMESTAMP '2007-03-31 12:59:30.123'", "INTERVAL '1 12:00:30.123' DAY TO SECOND", "'POINT (1 2)'",
     * "'LINESTRING (0 0, 0 1, 1 1)'", "'GEOMETRYCOLLECTION(POINT(2 5), POINT(3 5))'",
     * "'POLYGON((5 1, 5 5, 9 7, 10 1, 5 1),(6 2, 6 3, 7 3, 7 2, 6 2))'",
     * "'MULTIPOLYGON(((0 0, 0 2, 2 2, 3 1, 0 0)), ((4 6, 8 9, 12 5, 4 6), (8 6, 9 6, 9 7, 8 7, 8 6)))'",
     * "'MULTILINESTRING((0 1, 2 3, 1 6), (4 4, 5 5))'");
     */
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
