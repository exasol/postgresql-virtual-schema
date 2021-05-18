package com.exasol.adapter.dialects.postgresql.installer;

import static com.exasol.adapter.dialects.postgresql.installer.PostgresqlVirtualSchemaInstallerConstants.*;
import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
    void testInstallVirtualSchema() throws SQLException, BucketAccessException, InterruptedException, TimeoutException,
            ParseException, IOException {
        final String virtualSchemaName = "POSTGRES_VIRTUAL_SCHEMA_1";
        final String[] args = new String[] { //
                "--" + VIRTUAL_SCHEMA_JAR_NAME_KEY, "virtual-schema-dist-9.0.1-postgresql-2.0.0.jar", //
                "--" + VIRTUAL_SCHEMA_JAR_PATH_KEY, "target", //
                "--" + JDBC_DRIVER_NAME_KEY, "postgresql.jar", //
                "--" + JDBC_DRIVER_PATH_KEY, "target/postgresql-driver", //
                "--" + EXA_IP_KEY, "localhost", //
                "--" + EXA_PORT_KEY, EXASOL.getMappedPort(8563).toString(), //
                "--" + EXA_BUCKET_FS_PORT_KEY, EXASOL.getMappedPort(2580).toString(), //
                "--" + EXA_BUCKET_NAME_KEY, EXASOL.getDefaultBucket().getBucketName(), //
                "--" + EXA_SCHEMA_NAME_KEY, EXASOL_SCHEMA_NAME, //
                "--" + EXA_ADAPTER_NAME_KEY, EXASOL_ADAPTER_NAME, //
                "--" + EXA_CONNECTION_NAME_KEY, POSTGRES_JDBC_CONNECTION, //
                "--" + EXA_VIRTUAL_SCHEMA_NAME_KEY, virtualSchemaName, //
                "--" + POSTGRES_IP_KEY, EXASOL.getHostIp(), //
                "--" + POSTGRES_PORT_KEY, POSTGRES.getMappedPort(5432).toString(), //
                "--" + POSTGRES_DATABASE_NAME_KEY, POSTGRES.getDatabaseName(), //
                "--" + POSTGRES_MAPPED_SCHEMA_KEY, POSTGRES_SCHEMA, //
                "--" + ADDITIONAL_PROPERTY_KEY, "TABLE_FILTER='" + SIMPLE_POSTGRES_TABLE + "'", //
                "--" + ADDITIONAL_PROPERTY_KEY, "EXCLUDED_CAPABILITIES='LIMIT'" //
        };
        assertVirtualSchemaWasCreated(virtualSchemaName, args);
    }

    @Test
    void testInstallVirtualSchemaWithDefaultValues() throws SQLException, BucketAccessException, InterruptedException,
            TimeoutException, ParseException, IOException {
        final String virtualSchemaName = "POSTGRES_VIRTUAL_SCHEMA_2";
        final String[] args = new String[] { //
                "--" + VIRTUAL_SCHEMA_JAR_PATH_KEY, "target", //
                "--" + JDBC_DRIVER_PATH_KEY, "target/postgresql-driver", //
                "--" + EXA_PORT_KEY, EXASOL.getMappedPort(8563).toString(), //
                "--" + EXA_BUCKET_FS_PORT_KEY, EXASOL.getMappedPort(2580).toString(), //
                "--" + EXA_VIRTUAL_SCHEMA_NAME_KEY, virtualSchemaName, //
                "--" + POSTGRES_IP_KEY, EXASOL.getHostIp(), //
                "--" + POSTGRES_PORT_KEY, POSTGRES.getMappedPort(5432).toString(), //
                "--" + POSTGRES_DATABASE_NAME_KEY, POSTGRES.getDatabaseName(), //
                "--" + POSTGRES_MAPPED_SCHEMA_KEY, POSTGRES_SCHEMA //
        };
        assertVirtualSchemaWasCreated(virtualSchemaName, args);
    }

    private void assertVirtualSchemaWasCreated(final String virtualSchemaName, final String[] args)
            throws ParseException, SQLException, BucketAccessException, InterruptedException, TimeoutException,
            IOException {
        final String credentials = EXASOL_USERNAME_KEY + "=" + EXASOL.getUsername() + "\n" //
                + EXASOL_PASSWORD_KEY + "=" + EXASOL.getPassword() + "\n" //
                + EXASOL_BUCKET_WRITE_PASSWORD_KEY + "=" + EXASOL.getDefaultBucket().getWritePassword() + "\n" //
                + POSTGRES_USERNAME_KEY + "=" + POSTGRES.getUsername() + "\n" //
                + POSTGRES_PASSWORD_KEY + "=" + POSTGRES.getPassword() + "\n";
        final Path tempFile = Files.createTempFile("installer_credentials", "temp");
        Files.write(tempFile, credentials.getBytes());
        final String[] newArgs = new String[args.length + 2];
        System.arraycopy(args, 0, newArgs, 0, args.length);
        newArgs[newArgs.length - 2] = "--" + CREDENTIALS_FILE_KEY;
        newArgs[newArgs.length - 1] = tempFile.toString();
        Installer.main(newArgs);
        final ResultSet actualResultSet = EXASOL.createConnection().createStatement()
                .executeQuery("SELECT * FROM " + virtualSchemaName + "." + SIMPLE_POSTGRES_TABLE);
        assertThat(actualResultSet, table().row(1).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
    }
}
