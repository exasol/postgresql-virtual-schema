package com.exasol.adapter.dialects.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import com.exasol.adapter.commontests.scalarfunction.ScalarFunctionsTestBase;
import com.exasol.adapter.commontests.scalarfunction.TestSetup;
import com.exasol.adapter.commontests.scalarfunction.virtualschematestsetup.*;
import com.exasol.adapter.commontests.scalarfunction.virtualschematestsetup.request.Column;
import com.exasol.adapter.commontests.scalarfunction.virtualschematestsetup.request.TableRequest;
import com.exasol.adapter.metadata.DataType;
import com.exasol.closeafterall.CloseAfterAll;
import com.exasol.closeafterall.CloseAfterAllExtension;
import com.exasol.dbbuilder.dialects.Schema;
import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;
import com.exasol.dbbuilder.dialects.postgres.PostgreSqlObjectFactory;

@ExtendWith({ CloseAfterAllExtension.class })
class PostgreSQLScalarFunctionsIT extends ScalarFunctionsTestBase {
    @CloseAfterAll
    private static final PostgresVirtualSchemaIntegrationTestSetup SETUP = new PostgresVirtualSchemaIntegrationTestSetup();

    @Override
    protected TestSetup getTestSetup() {
        final PostgreSqlObjectFactory postgresFactory = SETUP.getPostgresFactory();
        return new TestSetup() {

            @Override
            public VirtualSchemaTestSetupProvider getVirtualSchemaTestSetupProvider() {
                return (final CreateVirtualSchemaTestSetupRequest request) -> {
                    final Schema postgresSchema = postgresFactory.createSchema(getUniqueIdentifier());
                    for (final TableRequest tableRequest : request.getTableRequests()) {
                        final Table.Builder tableBuilder = postgresSchema
                                .createTableBuilder(tableRequest.getName().toLowerCase());
                        for (final Column column : tableRequest.getColumns()) {
                            tableBuilder.column(column.getName().toLowerCase(), column.getType());
                        }
                        final Table table = tableBuilder.build();
                        for (final List<Object> row : tableRequest.getRows()) {
                            table.insert(row.toArray());
                        }
                    }

                    final VirtualSchema virtualSchema = SETUP.createVirtualSchema(postgresSchema.getName(), Collections.emptyMap());

                    return new PostgreSQLSingleTableVirtualSchemaTestSetup(virtualSchema, postgresSchema);
                };
            }

            @Override
            public String getExternalTypeFor(final DataType exasolType) {
                switch (exasolType.getExaDataType()) {
                case VARCHAR:
                    return "VARCHAR(" + exasolType.getSize() + ")";
                case DOUBLE:
                    return "DOUBLE PRECISION";
                case DECIMAL:
                    if (exasolType.getScale() == 0) {
                        return "INTEGER";
                    } else {
                        return exasolType.toString();
                    }
                default:
                    return exasolType.toString();
                }
            }

            @Override
            public Set<String> getDialectSpecificExcludes() {
                return Set.of(
                        // expected was a value close to <1970-03-01> (tolerance: +/- <0.00010>) but was
                        // "1970-03-01T00:00:00Z"
                        "add_months",
                        // expected was a value close to <1970-01-01> (tolerance: +/- <0.00010>) but was
                        // "1970-01-01T00:00:00Z"
                        "least",
                        // expected was a value close to <1970-01-15> (tolerance: +/- <0.00010>) but was
                        // "1970-01-15T00:00:00Z"
                        "add_weeks",
                        // expected was a value close to <1970-01-03> (tolerance: +/- <0.00010>) but was
                        // "1970-01-03T00:00:00Z"
                        "add_days",
                        // expected was a value close to <1972-01-01> (tolerance: +/- <0.00010>) but was
                        // "1972-01-01T00:00:00Z"
                        "add_years");
            }

            @Override
            public Connection createExasolConnection() throws SQLException {
                return SETUP.getExasolContainer().createConnection();
            }
        };
    }

    private static class PostgreSQLSingleTableVirtualSchemaTestSetup implements VirtualSchemaTestSetup {
        private final VirtualSchema virtualSchema;
        private final Schema postgresqlSchema;

        private PostgreSQLSingleTableVirtualSchemaTestSetup(final VirtualSchema virtualSchema,
                final Schema postgresqlSchema) {
            this.virtualSchema = virtualSchema;
            this.postgresqlSchema = postgresqlSchema;
        }

        @Override
        public String getFullyQualifiedName() {
            return this.virtualSchema.getFullyQualifiedName();
        }

        @Override
        public void close() {
            this.virtualSchema.drop();
            this.postgresqlSchema.drop();
        }
    }

    @BeforeAll
    static void beforeAll() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }
}
