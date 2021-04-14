package com.exasol.adapter.dialects.postgresql.installer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import com.exasol.bucketfs.BucketAccessException;
import com.exasol.bucketfs.WriteEnabledBucket;

/**
 * This class contains Postgres Virtual Schema installation logic.
 */
public class Installer {
    private static final Logger LOGGER = Logger.getLogger(Installer.class.getName());
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private static final String VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION = "virtual-schema-dist-9.0.1-postgresql-2.0.0.jar";
    private static final Path PATH_TO_VIRTUAL_SCHEMAS_JAR = Path.of("target", VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
    private static final String JDBC_DRIVER_NAME = "postgresql.jar";
    private static final Path JDBC_DRIVER_PATH = Path.of("target/postgresql-driver/" + JDBC_DRIVER_NAME);

    private final String exasolIpAddress;
    private final int exasolBucketFsPort;
    private final int exasolDatabasePort;
    private final String bucketName;
    private final String bucketWritePassword;
    private final String exasolUser;
    private final String exasolPassword;

    private final String postgresIpAddress;
    private final String postgresPort;
    private final String postgresDatabaseName;
    private final String postgresUsername;
    private final String postgresPassword;
    private final String postgresMappedSchema;

    private final String exasolSchemaName;
    private final String exasolAdapterName;
    private final String exasolConnectionName;
    private final String virtualSchemaName;

    private Installer(final Builder builder) {
        this.exasolIpAddress = builder.exasolIpAddress;
        this.exasolBucketFsPort = builder.exasolBucketFsPort;
        this.exasolDatabasePort = builder.exasolDatabasePort;
        this.bucketName = builder.bucketName;
        this.bucketWritePassword = builder.bucketWritePassword;
        this.exasolUser = builder.exasolUser;
        this.exasolPassword = builder.exasolPassword;

        this.postgresIpAddress = builder.postgresIpAddress;
        this.postgresPort = builder.postgresPort;
        this.postgresDatabaseName = builder.postgresDatabaseName;
        this.postgresUsername = builder.postgresUsername;
        this.postgresPassword = builder.postgresPassword;
        this.postgresMappedSchema = builder.postgresMappedSchema;

        this.exasolSchemaName = builder.exasolSchemaName;
        this.exasolAdapterName = builder.exasolAdapterName;
        this.exasolConnectionName = builder.exasolConnectionName;
        this.virtualSchemaName = builder.virtualSchemaName;
    }

    /**
     * Install Postgres Virtual Schema to the Exasol database.
     */
    public void install() throws SQLException, BucketAccessException, InterruptedException, TimeoutException {
        uploadFilesToBucket();
        try (final Connection connection = DriverManager.getConnection(
                "jdbc:exa:" + this.exasolIpAddress + ":" + this.exasolDatabasePort, this.exasolUser,
                this.exasolPassword); final Statement statement = connection.createStatement()) {
            installVirtualSchema(statement);
        }
    }

    private void uploadFilesToBucket() throws BucketAccessException, InterruptedException, TimeoutException {
        final WriteEnabledBucket bucket = getBucket();
        uploadVsJarToBucket(bucket);
        uploadDriverToBucket(bucket);
    }

    private WriteEnabledBucket getBucket() {
        return WriteEnabledBucket.builder()//
                .ipAddress(this.exasolIpAddress) //
                .httpPort(this.exasolBucketFsPort) //
                .name(this.bucketName) //
                .writePassword(this.bucketWritePassword) //
                .build();
    }

    private void installVirtualSchema(final Statement statement) throws SQLException {
        createSchema(statement);
        createAdapterScript(statement);
        createConnection(statement);
        createVirtualSchema(statement);
    }

    private void createSchema(final Statement statement) throws SQLException {
        statement.execute("CREATE SCHEMA IF NOT EXISTS " + this.exasolSchemaName);
    }

    private void createAdapterScript(final Statement statement) throws SQLException {
        final String createAdapterScriptStatement = prepareAdapterScriptStatement(this.exasolSchemaName,
                this.exasolAdapterName);
        LOGGER.info(() -> "Installing adapter script with the following command: " + LINE_SEPARATOR
                + createAdapterScriptStatement);
        statement.execute(createAdapterScriptStatement);
    }

    private void createConnection(final Statement statement) throws SQLException {
        final String connectionString = "jdbc:postgresql://" + this.postgresIpAddress + ":" + this.postgresPort + "/"
                + this.postgresDatabaseName;
        statement.execute("CREATE OR REPLACE CONNECTION " + this.exasolConnectionName + " TO '" + connectionString
                + "' USER '" + this.postgresUsername + "' IDENTIFIED BY '" + this.postgresPassword + "'");
    }

    private void createVirtualSchema(final Statement statement) throws SQLException {
        final String createVirtualSchemaStatement = prepareVirtualSchemaStatement();
        LOGGER.info(() -> "Installing virtual schema with the following command: " + LINE_SEPARATOR
                + createVirtualSchemaStatement);
        statement.execute(createVirtualSchemaStatement);
    }

    private String prepareVirtualSchemaStatement() {
        final String createVirtualSchemaStatement = "CREATE VIRTUAL SCHEMA " + this.virtualSchemaName + LINE_SEPARATOR //
                + "    USING " + this.exasolSchemaName + "." + this.exasolAdapterName + LINE_SEPARATOR //
                + "    WITH" + LINE_SEPARATOR //
                + "    SCHEMA_NAME = '" + this.postgresMappedSchema + "'" + LINE_SEPARATOR //
                + "    CONNECTION_NAME = '" + this.exasolConnectionName + "';" + LINE_SEPARATOR;
        return createVirtualSchemaStatement;
    }

    private String prepareAdapterScriptStatement(final String schemaName, final String adapterName) {
        final String createAdapterScriptStatement = "CREATE OR REPLACE JAVA ADAPTER SCRIPT " + schemaName + "."
                + adapterName + " AS" + LINE_SEPARATOR //
                + "  %scriptclass com.exasol.adapter.RequestDispatcher;" + LINE_SEPARATOR //
                + "%jar /buckets/bfsdefault/" + this.bucketName + "/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION + ";"
                + LINE_SEPARATOR //
                + "%jar /buckets/bfsdefault/" + this.bucketName + "/" + JDBC_DRIVER_NAME + ";";
        return createAdapterScriptStatement;
    }

    private void uploadVsJarToBucket(final WriteEnabledBucket bucket)
            throws BucketAccessException, InterruptedException, TimeoutException {
        bucket.uploadFileNonBlocking(PATH_TO_VIRTUAL_SCHEMAS_JAR, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
    }

    private void uploadDriverToBucket(final WriteEnabledBucket bucket)
            throws BucketAccessException, InterruptedException, TimeoutException {
        bucket.uploadFileNonBlocking(JDBC_DRIVER_PATH, JDBC_DRIVER_NAME);
    }

    public static Builder builder() {
        return new Builder();
    }

    static class Builder {
        private String exasolIpAddress = "localhost";
        private int exasolBucketFsPort = 2580;
        private int exasolDatabasePort = 8563;
        private String bucketName = "default";
        private String bucketWritePassword = "write";
        private String exasolUser = "sys";
        private String exasolPassword = "exasol";

        private String postgresIpAddress = "localhost";
        private String postgresPort = "5432";
        private String postgresUsername = "postgres";
        private String postgresPassword = "admin";
        private String postgresDatabaseName = "postgres";
        private String postgresMappedSchema = "";

        private String exasolSchemaName = "ADAPTER";
        private String exasolAdapterName = "POSTGRES_ADAPTER_SCRIPT";
        private String exasolConnectionName = "POSTGRES_JDBC_CONNECTION";
        private String virtualSchemaName = "POSTGRES_VIRTUAL_SCHEMA";

        public Builder exasolIpAddress(final String exasolIpAddress) {
            if (exasolIpAddress != null && !exasolIpAddress.isEmpty()) {
                this.exasolIpAddress = exasolIpAddress;
            }
            return this;
        }

        public Builder exasolBucketFsPort(final String exasolBucketFsPort) {
            if (exasolBucketFsPort != null && !exasolBucketFsPort.isEmpty()) {
                this.exasolBucketFsPort = Integer.parseInt(exasolBucketFsPort);
            }
            return this;
        }

        public Builder exasolDatabasePort(final String exasolDatabasePort) {
            if (exasolDatabasePort != null && !exasolDatabasePort.isEmpty()) {
                this.exasolDatabasePort = Integer.parseInt(exasolDatabasePort);
            }
            return this;
        }

        public Builder bucketName(final String bucketName) {
            if (bucketName != null && !bucketName.isEmpty()) {
                this.bucketName = bucketName;
            }
            return this;
        }

        public Builder bucketWritePassword(final String bucketWritePassword) {
            if (this.bucketName != null) {
                this.bucketWritePassword = bucketWritePassword;
            }
            return this;
        }

        public Builder exasolUser(final String exasolUser) {
            if (exasolUser != null && !exasolUser.isEmpty()) {
                this.exasolUser = exasolUser;
            }
            return this;
        }

        public Builder exasolPassword(final String exasolPassword) {
            if (exasolPassword != null && !exasolPassword.isEmpty()) {
                this.exasolPassword = exasolPassword;
            }
            return this;
        }

        public Builder postgresIpAddress(final String postgresIpAddress) {
            if (postgresIpAddress != null && !postgresIpAddress.isEmpty()) {
                this.postgresIpAddress = postgresIpAddress;
            }
            return this;
        }

        public Builder postgresPort(final String postgresPort) {
            if (postgresPort != null && !postgresPort.isEmpty()) {
                this.postgresPort = postgresPort;
            }
            return this;
        }

        public Builder postgresDatabaseName(final String postgresDatabaseName) {
            if (postgresDatabaseName != null && !postgresDatabaseName.isEmpty()) {
                this.postgresDatabaseName = postgresDatabaseName;
            }
            return this;
        }

        public Builder postgresUsername(final String postgresUsername) {
            if (postgresUsername != null && !postgresUsername.isEmpty()) {
                this.postgresUsername = postgresUsername;
            }
            return this;
        }

        public Builder postgresPassword(final String postgresPassword) {
            if (postgresPassword != null && !postgresPassword.isEmpty()) {
                this.postgresPassword = postgresPassword;
            }
            return this;
        }

        public Builder postgresMappedSchema(final String postgresMappedSchema) {
            if (postgresMappedSchema != null && !postgresMappedSchema.isEmpty()) {
                this.postgresMappedSchema = postgresMappedSchema;
            }
            return this;
        }

        public Builder exasolSchemaName(final String exasolSchemaName) {
            if (exasolSchemaName != null && !exasolSchemaName.isEmpty()) {
                this.exasolSchemaName = exasolSchemaName;
            }
            return this;
        }

        public Builder exasolAdapterName(final String exasolAdapterName) {
            if (exasolAdapterName != null && !exasolAdapterName.isEmpty()) {
                this.exasolAdapterName = exasolAdapterName;
            }
            return this;
        }

        public Builder exasolConnectionName(final String exasolConnectionName) {
            if (exasolConnectionName != null && !exasolConnectionName.isEmpty()) {
                this.exasolConnectionName = exasolConnectionName;
            }
            return this;
        }

        public Builder virtualSchemaName(final String virtualSchemaName) {
            if (virtualSchemaName != null && !virtualSchemaName.isEmpty()) {
                this.virtualSchemaName = virtualSchemaName;
            }
            return this;
        }

        public Installer build() {
            return new Installer(this);
        }
    }
}