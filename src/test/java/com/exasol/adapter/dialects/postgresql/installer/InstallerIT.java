package com.exasol.adapter.dialects.postgresql.installer;

import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.ParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.matcher.TypeMatchMode;

@Tag("integration")
@Testcontainers
class InstallerIT {
    public static final String SCHEMA_POSTGRES = "schema_postgres";
    public static final String TABLE_POSTGRES_SIMPLE = "table_postgres_simple";
    public static final String EXASOL_DOCKER_IMAGE_REFERENCE = "7.0.5";
    public static final String POSTGRES_CONTAINER_NAME = "postgres:13.1";

    @Container
    private static final PostgreSQLContainer<? extends PostgreSQLContainer<?>> POSTGRES = new PostgreSQLContainer<>(
            POSTGRES_CONTAINER_NAME);
    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> EXASOL = new ExasolContainer<>(
            EXASOL_DOCKER_IMAGE_REFERENCE).withReuse(true);

    @BeforeAll
    static void beforeAll() throws SQLException {
        final Statement statementPostgres = POSTGRES.createConnection("").createStatement();
        statementPostgres.execute("CREATE SCHEMA " + SCHEMA_POSTGRES);
        createPostgresTestTableSimple(statementPostgres);
    }

    private static void createPostgresTestTableSimple(final Statement statementPostgres) throws SQLException {
        final String qualifiedTableName = SCHEMA_POSTGRES + "." + TABLE_POSTGRES_SIMPLE;
        statementPostgres.execute("CREATE TABLE " + qualifiedTableName + " (x INT)");
        statementPostgres.execute("INSERT INTO " + qualifiedTableName + " VALUES (1)");
    }

    @Test
    void testInstallVirtualSchema()
            throws SQLException, BucketAccessException, InterruptedException, TimeoutException, ParseException {
        final String virtualSchemaName = "POSTGRES_VIRTUAL_SCHEMA";
        final String[] args = new String[] { //
                "-exasolBucketFsPort", EXASOL.getMappedPort(2580).toString(), //
                "-exasolDatabasePort", EXASOL.getMappedPort(8563).toString(), //
                "-bucketWritePassword", EXASOL.getDefaultBucket().getWritePassword(), //
                "-postgresIpAddress", EXASOL.getHostIp(), //
                "-postgresPort", POSTGRES.getMappedPort(5432).toString(), //
                "-postgresDatabaseName", POSTGRES.getDatabaseName(), //
                "-postgresUsername", POSTGRES.getUsername(), //
                "-postgresPassword", POSTGRES.getPassword(), //
                "-postgresMappedSchema", SCHEMA_POSTGRES, //
                "-virtualSchemaName", virtualSchemaName //
        };
        final Installer installer = new UserInputParser().parseUserInput(args);
        installer.install();
        final ResultSet actualResultSet = EXASOL.createConnection().createStatement()
                .executeQuery("SELECT * FROM " + virtualSchemaName + "." + TABLE_POSTGRES_SIMPLE);
        assertThat(actualResultSet, table().row(1).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
    }
}