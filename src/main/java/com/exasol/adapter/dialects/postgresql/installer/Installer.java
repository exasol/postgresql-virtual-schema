package com.exasol.adapter.dialects.postgresql.installer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import org.apache.commons.cli.ParseException;

import com.exasol.bucketfs.BucketAccessException;
import com.exasol.bucketfs.WriteEnabledBucket;

/**
 * This class contains Postgres Virtual Schema installation logic.
 */
public class Installer {
    private static final Logger LOGGER = Logger.getLogger(Installer.class.getName());
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");

    // Files related fields
    private final String virtualSchemaJarName;
    private final Path virtualSchemaJarPath;
    private final String jdbcDriverName;
    private final Path jdbcDriverPath;

    // Credentials
    private final String exaUsername;
    private final String exaPassword;
    private final String exaBucketWritePassword;
    private final String postgresUsername;
    private final String postgresPassword;

    // Exasol related fields
    private final String exaIp;
    private final int exaPort;
    private final int exaBucketFsPort;
    private final String exaBucketName;
    private final String exaSchemaName;
    private final String exaAdapterName;
    private final String exaConnectionName;
    private final String exaVirtualSchemaName;

    // Postgres related fields
    private final String postgresIp;
    private final String postgresPort;
    private final String postgresDatabaseName;
    private final String postgresMappedSchema;

    private Installer(final Builder builder) {
        this.virtualSchemaJarName = builder.virtualSchemaJarName;
        this.virtualSchemaJarPath = Path.of(builder.virtualSchemaJarPath, this.virtualSchemaJarName);
        this.jdbcDriverName = builder.jdbcDriverName;
        this.jdbcDriverPath = Path.of(builder.jdbcDriverPath, this.jdbcDriverName);

        this.exaUsername = builder.exaUsername;
        this.exaPassword = builder.exaPassword;
        this.postgresUsername = builder.postgresUsername;
        this.postgresPassword = builder.postgresPassword;

        this.exaIp = builder.exaIp;
        this.exaPort = builder.exaPort;
        this.exaBucketFsPort = builder.exaBucketFsPort;
        this.exaBucketName = builder.exaBucketName;
        this.exaBucketWritePassword = builder.exaBucketWritePassword;
        this.exaSchemaName = builder.exaSchemaName;
        this.exaAdapterName = builder.exaAdapterName;
        this.exaConnectionName = builder.exaConnectionName;
        this.exaVirtualSchemaName = builder.exaVirtualSchemaName;

        this.postgresIp = builder.postgresIp;
        this.postgresPort = builder.postgresPort;
        this.postgresDatabaseName = builder.postgresDatabaseName;
        this.postgresMappedSchema = builder.postgresMappedSchema;
    }

    /**
     * Install Postgres Virtual Schema to the Exasol database.
     */
    public void install() throws SQLException, BucketAccessException, InterruptedException, TimeoutException {
        uploadFilesToBucket();
        try (final Connection connection = DriverManager.getConnection("jdbc:exa:" + this.exaIp + ":" + this.exaPort,
                this.exaUsername, this.exaPassword); final Statement statement = connection.createStatement()) {
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
                .ipAddress(this.exaIp) //
                .httpPort(this.exaBucketFsPort) //
                .name(this.exaBucketName) //
                .writePassword(this.exaBucketWritePassword) //
                .build();
    }

    private void installVirtualSchema(final Statement statement) throws SQLException {
        createSchema(statement);
        createAdapterScript(statement);
        createConnection(statement);
        createVirtualSchema(statement);
    }

    private void createSchema(final Statement statement) throws SQLException {
        statement.execute("CREATE SCHEMA IF NOT EXISTS " + this.exaSchemaName);
    }

    private void createAdapterScript(final Statement statement) throws SQLException {
        final String createAdapterScriptStatement = prepareAdapterScriptStatement(this.exaSchemaName,
                this.exaAdapterName);
        LOGGER.info(() -> "Installing adapter script with the following command: " + LINE_SEPARATOR
                + createAdapterScriptStatement);
        statement.execute(createAdapterScriptStatement);
    }

    private void createConnection(final Statement statement) throws SQLException {
        final String connectionString = "jdbc:postgresql://" + this.postgresIp + ":" + this.postgresPort + "/"
                + this.postgresDatabaseName;
        LOGGER.info(() -> "Creating connection object with the following connection string: " + LINE_SEPARATOR
                + connectionString);
        statement.execute("CREATE OR REPLACE CONNECTION " + this.exaConnectionName + " TO '" + connectionString
                + "' USER '" + this.postgresUsername + "' IDENTIFIED BY '" + this.postgresPassword + "'");
    }

    private void createVirtualSchema(final Statement statement) throws SQLException {
        final String createVirtualSchemaStatement = prepareVirtualSchemaStatement();
        LOGGER.info(() -> "Installing virtual schema with the following command: " + LINE_SEPARATOR
                + createVirtualSchemaStatement);
        statement.execute(createVirtualSchemaStatement);
    }

    private String prepareVirtualSchemaStatement() {
        return "CREATE VIRTUAL SCHEMA " + this.exaVirtualSchemaName + LINE_SEPARATOR //
                + "    USING " + this.exaSchemaName + "." + this.exaAdapterName + LINE_SEPARATOR //
                + "    WITH" + LINE_SEPARATOR //
                + "    SCHEMA_NAME = '" + this.postgresMappedSchema + "'" + LINE_SEPARATOR //
                + "    CONNECTION_NAME = '" + this.exaConnectionName + "';" + LINE_SEPARATOR;
    }

    private String prepareAdapterScriptStatement(final String schemaName, final String adapterName) {
        return "CREATE OR REPLACE JAVA ADAPTER SCRIPT " + schemaName + "." + adapterName + " AS" + LINE_SEPARATOR //
                + "  %scriptclass com.exasol.adapter.RequestDispatcher;" + LINE_SEPARATOR //
                + "%jar /buckets/bfsdefault/" + this.exaBucketName + "/" + this.virtualSchemaJarName + ";"
                + LINE_SEPARATOR //
                + "%jar /buckets/bfsdefault/" + this.exaBucketName + "/" + this.jdbcDriverName + ";";
    }

    private void uploadVsJarToBucket(final WriteEnabledBucket bucket)
            throws BucketAccessException, InterruptedException, TimeoutException {
        bucket.uploadFileNonBlocking(this.virtualSchemaJarPath, this.virtualSchemaJarName);
    }

    private void uploadDriverToBucket(final WriteEnabledBucket bucket)
            throws BucketAccessException, InterruptedException, TimeoutException {
        bucket.uploadFileNonBlocking(this.jdbcDriverPath, this.jdbcDriverName);
    }

    public static Builder builder(final User exaUser, final User postgresUser, final User bucket) {
        return new Builder(exaUser, postgresUser, bucket);
    }

    static class Builder {
        private String virtualSchemaJarName = "virtual-schema-dist-9.0.1-postgresql-2.0.0.jar";
        private String virtualSchemaJarPath = "";
        private String jdbcDriverName = "postgresql.jar";
        private String jdbcDriverPath = "";

        private final String exaUsername;
        private final String exaPassword;
        private final String exaBucketWritePassword;
        private final String postgresUsername;
        private final String postgresPassword;

        private String exaIp = "localhost";
        private int exaPort = 8563;
        private int exaBucketFsPort = 2580;
        private String exaBucketName = "default";
        private String exaSchemaName = "ADAPTER";
        private String exaAdapterName = "POSTGRES_ADAPTER_SCRIPT";
        private String exaConnectionName = "POSTGRES_JDBC_CONNECTION";
        private String exaVirtualSchemaName = "POSTGRES_VIRTUAL_SCHEMA";

        private String postgresIp = "localhost";
        private String postgresPort = "5432";
        private String postgresDatabaseName = "postgres";
        private String postgresMappedSchema = "";

        public Builder(final User exaUser, final User postgresUser, final User bucket) {
            this.exaUsername = exaUser.getUsername();
            this.exaPassword = exaUser.getPassword();
            this.postgresUsername = postgresUser.getUsername();
            this.postgresPassword = postgresUser.getPassword();
            this.exaBucketWritePassword = bucket.getPassword();
        }

        public Builder virtualSchemaJarName(final String virtualSchemaJarName) {
            if (virtualSchemaJarName != null && !virtualSchemaJarName.isEmpty()) {
                this.virtualSchemaJarName = virtualSchemaJarName;
            }
            return this;
        }

        public Builder virtualSchemaJarPath(final String virtualSchemaJarPath) {
            if (virtualSchemaJarPath != null && !virtualSchemaJarPath.isEmpty()) {
                this.virtualSchemaJarPath = virtualSchemaJarPath;
            }
            return this;
        }

        public Builder jdbcDriverName(final String jdbcDriverName) {
            if (jdbcDriverName != null && !jdbcDriverName.isEmpty()) {
                this.jdbcDriverName = jdbcDriverName;
            }
            return this;
        }

        public Builder jdbcDriverPath(final String jdbcDriverPath) {
            if (jdbcDriverPath != null && !jdbcDriverPath.isEmpty()) {
                this.jdbcDriverPath = jdbcDriverPath;
            }
            return this;
        }

        public Builder exaIp(final String exaIp) {
            if (exaIp != null && !exaIp.isEmpty()) {
                this.exaIp = exaIp;
            }
            return this;
        }

        public Builder exaPort(final String exaPort) {
            if (exaPort != null && !exaPort.isEmpty()) {
                this.exaPort = Integer.parseInt(exaPort);
            }
            return this;
        }

        public Builder exaBucketFsPort(final String exaBucketFsPort) {
            if (exaBucketFsPort != null && !exaBucketFsPort.isEmpty()) {
                this.exaBucketFsPort = Integer.parseInt(exaBucketFsPort);
            }
            return this;
        }

        public Builder exaBucketName(final String exaBucketName) {
            if (exaBucketName != null && !exaBucketName.isEmpty()) {
                this.exaBucketName = exaBucketName;
            }
            return this;
        }

        public Builder exaSchemaName(final String exaSchemaName) {
            if (exaSchemaName != null && !exaSchemaName.isEmpty()) {
                this.exaSchemaName = exaSchemaName;
            }
            return this;
        }

        public Builder exaAdapterName(final String exaAdapterName) {
            if (exaAdapterName != null && !exaAdapterName.isEmpty()) {
                this.exaAdapterName = exaAdapterName;
            }
            return this;
        }

        public Builder exaConnectionName(final String exaConnectionName) {
            if (exaConnectionName != null && !exaConnectionName.isEmpty()) {
                this.exaConnectionName = exaConnectionName;
            }
            return this;
        }

        public Builder exaVirtualSchemaName(final String exaVirtualSchemaName) {
            if (exaVirtualSchemaName != null && !exaVirtualSchemaName.isEmpty()) {
                this.exaVirtualSchemaName = exaVirtualSchemaName;
            }
            return this;
        }

        public Builder postgresIp(final String postgresIp) {
            if (postgresIp != null && !postgresIp.isEmpty()) {
                this.postgresIp = postgresIp;
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

        public Builder postgresMappedSchema(final String postgresMappedSchema) {
            if (postgresMappedSchema != null && !postgresMappedSchema.isEmpty()) {
                this.postgresMappedSchema = postgresMappedSchema;
            }
            return this;
        }

        public Installer build() {
            return new Installer(this);
        }
    }

    public static void main(final String[] args)
            throws ParseException, SQLException, BucketAccessException, InterruptedException, TimeoutException {
        final User exaUser = CredentialsProvider.getInstance().provideExasolUser();
        final User postgresUser = CredentialsProvider.getInstance().providePostgresUser();
        final User bucketUser = CredentialsProvider.getInstance().provideBucketUser();
        final Installer installer = new UserInputParser().parseUserInput(args, exaUser, postgresUser, bucket);
        installer.install();
    }
}
