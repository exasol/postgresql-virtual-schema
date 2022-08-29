package com.exasol.adapter.dialects.postgresql;

import static com.exasol.dbbuilder.dialects.exasol.AdapterScript.Language.JAVA;

import java.io.*;
import java.nio.file.Path;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.testcontainers.containers.PostgreSQLContainer;

import com.exasol.bucketfs.Bucket;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolService;
import com.exasol.dbbuilder.dialects.exasol.*;
import com.exasol.dbbuilder.dialects.postgres.PostgreSqlObjectFactory;
import com.exasol.errorreporting.ExaError;
import com.exasol.udfdebugging.UdfTestSetup;
import com.github.dockerjava.api.model.ContainerNetwork;

/**
 * This class contains the common integration test setup for all PostgreSQL virtual schemas.
 */
public class PostgresVirtualSchemaIntegrationTestSetup implements Closeable {
    private static final String VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION = "virtual-schema-dist-9.0.5-postgresql-2.0.4.jar";
    private static final Path PATH_TO_VIRTUAL_SCHEMAS_JAR = Path.of("target", VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
    private static final String SCHEMA_EXASOL = "SCHEMA_EXASOL";
    private static final String ADAPTER_SCRIPT_EXASOL = "ADAPTER_SCRIPT_EXASOL";
    private static final String EXASOL_DOCKER_IMAGE_REFERENCE = "7.1.6";
    private static final String POSTGRES_CONTAINER_NAME = "postgres:14.2";
    private static final String JDBC_DRIVER_NAME = "postgresql.jar";
    static final Path JDBC_DRIVER_PATH = Path.of("target/postgresql-driver/" + JDBC_DRIVER_NAME);
    private static final int POSTGRES_PORT = 5432;
    private final Statement postgresStatement;
    private final PostgreSQLContainer<? extends PostgreSQLContainer<?>> postgresqlContainer = new PostgreSQLContainer<>(
            POSTGRES_CONTAINER_NAME);
    private final ExasolContainer<? extends ExasolContainer<?>> exasolContainer = new ExasolContainer<>(
            EXASOL_DOCKER_IMAGE_REFERENCE).withRequiredServices(ExasolService.BUCKETFS, ExasolService.UDF)
                    .withReuse(true);
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
            uploadDriverToBucket(bucket);
            uploadVsJarToBucket(bucket);
            this.exasolConection = this.exasolContainer.createConnection("");
            this.exasolStatement = this.exasolConection.createStatement();
            this.postgresConnection = this.postgresqlContainer.createConnection("");
            this.postgresStatement = this.postgresConnection.createStatement();
            final UdfTestSetup udfTestSetup = new UdfTestSetup(getTestHostIpFromInsideExasol(),
                    this.exasolContainer.getDefaultBucket(), this.exasolConection);
            this.exasolFactory = new ExasolObjectFactory(this.exasolContainer.createConnection(""),
                    ExasolObjectConfiguration.builder().withJvmOptions(udfTestSetup.getJvmOptions()).build());
            final ExasolSchema exasolSchema = this.exasolFactory.createSchema(SCHEMA_EXASOL);
            this.postgresFactory = new PostgreSqlObjectFactory(this.postgresConnection);
            this.adapterScript = createAdapterScript(exasolSchema);
            final String connectionString = "jdbc:postgresql://" + this.exasolContainer.getHostIp() + ":"
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

    private static void uploadDriverToBucket(final Bucket bucket)
            throws InterruptedException, TimeoutException, BucketAccessException {
        try {
            bucket.uploadFile(JDBC_DRIVER_PATH, JDBC_DRIVER_NAME);
        } catch (final BucketAccessException | FileNotFoundException exception) {
            throw new IllegalStateException(
                    ExaError.messageBuilder("F-PGVS-8")
                            .message("An error occurred while uploading the jdbc driver to the bucket.")
                            .mitigation("Make sure the {{JDBC_DRIVER_PATH}} file exists.")
                            .parameter("JDBC_DRIVER_PATH", JDBC_DRIVER_PATH)
                            .mitigation("You can generate it by executing the integration test with maven.").toString(),
                    exception);
        }
    }

    private static void uploadVsJarToBucket(final Bucket bucket) {
        try {
            bucket.uploadFile(PATH_TO_VIRTUAL_SCHEMAS_JAR, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
        } catch (FileNotFoundException | BucketAccessException | TimeoutException exception) {
            throw new IllegalStateException("Failed to upload jar to bucket " + bucket, exception);
        }
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
                .properties(properties).build();
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

    private String getTestHostIpFromInsideExasol() {
        final Map<String, ContainerNetwork> networks = this.exasolContainer.getContainerInfo().getNetworkSettings()
                .getNetworks();
        if (networks.size() == 0) {
            return null;
        }
        return networks.values().iterator().next().getGateway();
    }
}
