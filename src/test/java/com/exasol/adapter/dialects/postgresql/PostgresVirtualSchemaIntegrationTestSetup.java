package com.exasol.adapter.dialects.postgresql;

import static com.exasol.dbbuilder.dialects.exasol.AdapterScript.Language.JAVA;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import com.exasol.bucketfs.Bucket;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.dbbuilder.dialects.exasol.*;
import com.exasol.dbbuilder.dialects.postgres.PostgreSqlObjectFactory;

/**
 * This class contains the common integration test setup for all PostgreSQL virtual schemas.
 */
public class PostgresVirtualSchemaIntegrationTestSetup implements Closeable {
    private static final String VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION = "virtual-schema-dist-8.0.0-postgresql-1.0.0.jar";
    private static final Path PATH_TO_VIRTUAL_SCHEMAS_JAR = Path.of("target", VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
    private static final String SCHEMA_EXASOL = "SCHEMA_EXASOL";
    private static final String ADAPTER_SCRIPT_EXASOL = "ADAPTER_SCRIPT_EXASOL";
    private static final String POSTGRES_DRIVER_NAME_AND_VERSION = "postgresql-42.2.5.jar";
    private static final Path PATH_TO_POSTGRES_DRIVER = Path.of("src", "test", "resources", "integration", "driver",
            "postgres", POSTGRES_DRIVER_NAME_AND_VERSION);
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLSqlDialectIT.class);
    private static final String EXASOL_DOCKER_IMAGE_REFERENCE = "7.0.5";
    private static final String POSTGRES_CONTAINER_NAME = "postgres:13.1";
    private static final String DOCKER_IP_ADDRESS = "172.17.0.1";
    private static final int POSTGRES_PORT = 5432;
    private final Statement postgresStatement;
    private final PostgreSQLContainer<? extends PostgreSQLContainer<?>> postgresqlContainer = new PostgreSQLContainer<>(
            POSTGRES_CONTAINER_NAME);
    private final ExasolContainer<? extends ExasolContainer<?>> exasolContainer = new ExasolContainer<>(
            EXASOL_DOCKER_IMAGE_REFERENCE) //
                    .withLogConsumer(new Slf4jLogConsumer(LOGGER)).withReuse(true);
    private final Connection exasolConection;
    private final Statement exasolStatement;
    private final AdapterScript adapterScript;
    private final ConnectionDefinition connectionDefinition;
    private final ExasolObjectFactory exasolFactory;
    private final PostgreSqlObjectFactory postgresFactory;
    private final Connection postgresConnection;
    private int virtualSchemaCounter = 0;

    PostgresVirtualSchemaIntegrationTestSetup() {
        try {
            this.exasolContainer.start();
            this.postgresqlContainer.start();
            final Bucket bucket = this.exasolContainer.getDefaultBucket();
            bucket.uploadFile(PATH_TO_POSTGRES_DRIVER, POSTGRES_DRIVER_NAME_AND_VERSION);
            uploadVsJarToBucket(bucket);
            this.exasolConection = this.exasolContainer.createConnection("");
            this.exasolStatement = this.exasolConection.createStatement();
            this.postgresConnection = this.postgresqlContainer.createConnection("");
            this.postgresStatement = this.postgresConnection.createStatement();
            this.exasolFactory = new ExasolObjectFactory(this.exasolContainer.createConnection(""));
            final ExasolSchema exasolSchema = this.exasolFactory.createSchema(SCHEMA_EXASOL);
            this.postgresFactory = new PostgreSqlObjectFactory(this.postgresConnection);
            this.adapterScript = createAdapterScript(exasolSchema);
            final String connectionString = "jdbc:postgresql://" + DOCKER_IP_ADDRESS + ":"
                    + this.postgresqlContainer.getMappedPort(POSTGRES_PORT) + "/"
                    + this.postgresqlContainer.getDatabaseName();
            this.connectionDefinition = this.exasolFactory.createConnectionDefinition("POSGRES_CONNECTION",
                    connectionString, this.postgresqlContainer.getUsername(), this.postgresqlContainer.getPassword());
        } catch (final SQLException | BucketAccessException | TimeoutException exception) {
            throw new IllegalStateException("Failed to created PostgreSQL test setup.", exception);
        } catch (final InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Thread was interrupted");
        }
    }

    private static void uploadVsJarToBucket(final Bucket bucket)
            throws InterruptedException, BucketAccessException, TimeoutException {
        bucket.uploadFile(PATH_TO_VIRTUAL_SCHEMAS_JAR, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
    }

    private AdapterScript createAdapterScript(final ExasolSchema schema) {
        final String content = "%scriptclass com.exasol.adapter.RequestDispatcher;\n" //
                + "%jar /buckets/bfsdefault/default/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION + ";\n";
        return schema.createAdapterScript(ADAPTER_SCRIPT_EXASOL, JAVA, content);
    }

    public PostgreSqlObjectFactory getPostgresFactory() {
        return this.postgresFactory;
    }

    public Statement getPostgresqlStatement() {
        return this.postgresStatement;
    }

    public Statement getExasolStatement() {
        return this.exasolStatement;
    }

    public ExasolContainer<? extends ExasolContainer<?>> getExasolContainer() {
        return this.exasolContainer;
    }

    public VirtualSchema createVirtualSchema(final String forPostgresSchema,
            final Map<String, String> additionalProperties) {
        final Map<String, String> properties = new HashMap<>(
                Map.of("CATALOG_NAME", this.postgresqlContainer.getDatabaseName(), //
                        "SCHEMA_NAME", forPostgresSchema));
        properties.putAll(additionalProperties);
        return this.exasolFactory.createVirtualSchemaBuilder("POSTGRES_VIRTUAL_SCHEMA_" + (this.virtualSchemaCounter++))
                .adapterScript(this.adapterScript).connectionDefinition(this.connectionDefinition)
                .dialectName("POSTGRESQL").properties(properties).build();
    }

    public ExasolObjectFactory getExasolFactory() {
        return this.exasolFactory;
    }

    @Override
    public void close() throws IOException {
        try {
            this.exasolStatement.close();
            this.exasolConection.close();
            this.postgresStatement.close();
            this.postgresConnection.close();
            this.exasolContainer.stop();
            this.postgresqlContainer.stop();
        } catch (final SQLException exception) {
            throw new IllegalStateException("Failed to stop test setup.", exception);
        }
    }
}
