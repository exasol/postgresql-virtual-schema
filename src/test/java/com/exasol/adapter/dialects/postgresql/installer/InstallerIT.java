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
    private static final String EXASOL_SCHEMA_NAME = "ADAPTER";
    private static final String EXASOL_ADAPTER_NAME = "POSTGRES_ADAPTER_SCRIPT";
    private static final String POSTGRES_JDBC_CONNECTION = "POSTGRES_JDBC_CONNECTION";
    private static final String POSTGRES_SCHEMA = "postgres_schema";
    private static final String SIMPLE_POSTGRES_TABLE = "simple_postgres_table";
    private static final String EXASOL_DOCKER_IMAGE_REFERENCE = "7.0.5";
    private static final String POSTGRES_CONTAINER_NAME = "postgres:13.1";

    @Container
    private static final PostgreSQLContainer<? extends PostgreSQLContainer<?>> POSTGRES = new PostgreSQLContainer<>(
            POSTGRES_CONTAINER_NAME);
    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> EXASOL = new ExasolContainer<>(
            EXASOL_DOCKER_IMAGE_REFERENCE).withReuse(true);

    @BeforeAll
    static void beforeAll() throws SQLException {
        final Statement statementPostgres = POSTGRES.createConnection("").createStatement();
        statementPostgres.execute("CREATE SCHEMA " + POSTGRES_SCHEMA);
        createPostgresTestTableSimple(statementPostgres);
    }

    private static void createPostgresTestTableSimple(final Statement statementPostgres) throws SQLException {
        final String qualifiedTableName = POSTGRES_SCHEMA + "." + SIMPLE_POSTGRES_TABLE;
        statementPostgres.execute("CREATE TABLE " + qualifiedTableName + " (x INT)");
        statementPostgres.execute("INSERT INTO " + qualifiedTableName + " VALUES (1)");
    }

    @Test
    void testInstallVirtualSchema()
            throws SQLException, BucketAccessException, InterruptedException, TimeoutException, ParseException {
        final String virtualSchemaName = "POSTGRES_VIRTUAL_SCHEMA_1";
        final String[] args = new String[] { //
                "-virtualSchemaJarName", "virtual-schema-dist-9.0.1-postgresql-2.0.0.jar", //
                "-virtualSchemaJarPath", "target", //
                "-jdbcDriverName", "postgresql.jar", //
                "-jdbcDriverPath", "target/postgresql-driver", //
                "-exaIp", "localhost", //
                "-exaPort", EXASOL.getMappedPort(8563).toString(), //
                "-exaBucketFsPort", EXASOL.getMappedPort(2580).toString(), //
                "-exaBucketName", EXASOL.getDefaultBucket().getBucketName(), //
                "-exaSchemaName", EXASOL_SCHEMA_NAME, //
                "-exaAdapterName", EXASOL_ADAPTER_NAME, //
                "-exaConnectionName", POSTGRES_JDBC_CONNECTION, //
                "-exaVirtualSchemaName", virtualSchemaName, //
                "-postgresIp", EXASOL.getHostIp(), //
                "-postgresPort", POSTGRES.getMappedPort(5432).toString(), //
                "-postgresDatabaseName", POSTGRES.getDatabaseName(), //
                "-postgresMappedSchema", POSTGRES_SCHEMA //
        };
        assertVirtualSchemaWasCreated(virtualSchemaName, args);
    }

    @Test
    void testInstallVirtualSchemaWithDefaultValues()
            throws SQLException, BucketAccessException, InterruptedException, TimeoutException, ParseException {
        final String virtualSchemaName = "POSTGRES_VIRTUAL_SCHEMA_2";
        final String[] args = new String[] { //
                "-virtualSchemaJarPath", "target", //
                "-jdbcDriverPath", "target/postgresql-driver", //
                "-exaPort", EXASOL.getMappedPort(8563).toString(), //
                "-exaBucketFsPort", EXASOL.getMappedPort(2580).toString(), //
                "-exaVirtualSchemaName", virtualSchemaName, //
                "-postgresIp", EXASOL.getHostIp(), //
                "-postgresPort", POSTGRES.getMappedPort(5432).toString(), //
                "-postgresDatabaseName", POSTGRES.getDatabaseName(), //
                "-postgresMappedSchema", POSTGRES_SCHEMA //
        };
        assertVirtualSchemaWasCreated(virtualSchemaName, args);
    }

    private void assertVirtualSchemaWasCreated(final String virtualSchemaName, final String[] args)
            throws ParseException, SQLException, BucketAccessException, InterruptedException, TimeoutException {
        final User exaUser = new User(EXASOL.getUsername(), EXASOL.getPassword());
        final User postgresUser = new User(POSTGRES.getUsername(), POSTGRES.getPassword());
        final User bucket = new User("", EXASOL.getDefaultBucket().getWritePassword());
        final Installer installer = new UserInputParser().parseUserInput(args, exaUser, postgresUser, bucket);
        installer.install();
        final ResultSet actualResultSet = EXASOL.createConnection().createStatement()
                .executeQuery("SELECT * FROM " + virtualSchemaName + "." + SIMPLE_POSTGRES_TABLE);
        assertThat(actualResultSet, table().row(1).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
    }
}
