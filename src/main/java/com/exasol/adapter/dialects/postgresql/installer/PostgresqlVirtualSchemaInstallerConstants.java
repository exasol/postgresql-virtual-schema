package com.exasol.adapter.dialects.postgresql.installer;

public class PostgresqlVirtualSchemaInstallerConstants {
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");
    public static final String CREDENTIALS_FILE_KEY = "credentials_file";
    public static final String CREDENTIALS_FILE_DESCRIPTION = "Path to the file where credentials are stored";
    public static final String CREDENTIALS_FILE_DEFAULT = FILE_SEPARATOR + ".virtual-schema-installer" + FILE_SEPARATOR
            + "credentials";

    public static final String EXASOL_USERNAME_KEY = "exasol_username";
    public static final String EXASOL_PASSWORD_KEY = "exasol_password";
    public static final String EXASOL_BUCKET_WRITE_PASSWORD_KEY = "exasol_bucket_write_password";
    public static final String POSTGRES_USERNAME_KEY = "postgres_username";
    public static final String POSTGRES_PASSWORD_KEY = "postgres_password";

    public static final String VIRTUAL_SCHEMA_JAR_NAME_KEY = "virtualSchemaJarName";
    public static final String VIRTUAL_SCHEMA_JAR_NAME_DEFAULT = "virtual-schema-dist-9.0.1-postgresql-2.0.0.jar";
    public static final String VIRTUAL_SCHEMA_JAR_NAME_DESCRIPTION = "Name of the Virtual Schema JAR file";

    public static final String VIRTUAL_SCHEMA_JAR_PATH_KEY = "virtualSchemaJarPath";
    public static final String VIRTUAL_SCHEMA_JAR_PATH_DEFAULT = "";
    public static final String VIRTUAL_SCHEMA_JAR_PATH_DESCRIPTION = "Path to the Virtual Schema JAR file";

    public static final String JDBC_DRIVER_NAME_KEY = "jdbcDriverName";
    public static final String JDBC_DRIVER_NAME_DEFAULT = "postgresql.jar";
    public static final String JDBC_DRIVER_NAME_DESCRIPTION = "Name of the PostgreSQL JDBC driver file";

    public static final String JDBC_DRIVER_PATH_KEY = "jdbcDriverPath";
    public static final String JDBC_DRIVER_PATH_DEFAULT = "";
    public static final String JDBC_DRIVER_PATH_DESCRIPTION = "Path to the PostgreSQL JDBC driver file";

    public static final String EXA_IP_KEY = "exaIp";
    public static final String EXA_IP_DEFAULT = "localhost";
    public static final String EXA_IP_DESCRIPTION = "An IP address to connect to the Exasol database";

    public static final String EXA_PORT_KEY = "exaPort";
    public static final String EXA_PORT_DEFAULT = "8563";
    public static final String EXA_PORT_DESCRIPTION = "A port on which the Exasol database is listening";

    public static final String EXA_BUCKET_FS_PORT_KEY = "exaBucketFsPort";
    public static final String EXA_BUCKET_FS_PORT_DEFAULT = "2580";
    public static final String EXA_BUCKET_FS_PORT_DESCRIPTION = "A port on which BucketFS is listening";

    public static final String EXA_BUCKET_NAME_KEY = "exaBucketName";
    public static final String EXA_BUCKET_NAME_DEFAULT = "default";
    public static final String EXA_BUCKET_NAME_DESCRIPTION = "A bucket name to upload jars";

    public static final String EXA_SCHEMA_NAME_KEY = "exaSchemaName";
    public static final String EXA_SCHEMA_NAME_DEFAULT = "ADAPTER";
    public static final String EXA_SCHEMA_NAME_DESCRIPTION = "A name for an Exasol schema that holds the adapter script";

    public static final String EXA_ADAPTER_NAME_KEY = "exaAdapterName";
    public static final String EXA_ADAPTER_NAME_DEFAULT = "POSTGRES_ADAPTER_SCRIPT";
    public static final String EXA_ADAPTER_NAME_DESCRIPTION = "A name for an Exasol adapter script";

    public static final String EXA_CONNECTION_NAME_KEY = "exaConnectionName";
    public static final String EXA_CONNECTION_NAME_DEFAULT = "POSTGRES_JDBC_CONNECTION";
    public static final String EXA_CONNECTION_NAME_DESCRIPTION = "A name for an Exasol connection to the Postgres database";

    public static final String EXA_VIRTUAL_SCHEMA_NAME_KEY = "exaVirtualSchemaName";
    public static final String EXA_VIRTUAL_SCHEMA_NAME_DEFAULT = "POSTGRES_VIRTUAL_SCHEMA";
    public static final String EXA_VIRTUAL_SCHEMA_NAME_DESCRIPTION = "A name for a virtual schema";

    public static final String POSTGRES_IP_KEY = "postgresIp";
    public static final String POSTGRES_IP_DEFAULT = "localhost";
    public static final String POSTGRES_IP_DESCRIPTION = "An IP address to connect to the PostgreSQL database";

    public static final String POSTGRES_PORT_KEY = "postgresPort";
    public static final String POSTGRES_PORT_DEFAULT = "5432";
    public static final String POSTGRES_PORT_DESCRIPTION = "A port on which the PostgreSQL database is listening";

    public static final String POSTGRES_DATABASE_NAME_KEY = "postgresDatabaseName";
    public static final String POSTGRES_DATABASE_NAME_DEFAULT = "postgres";
    public static final String POSTGRES_DATABASE_NAME_DESCRIPTION = "A PostgreSQL database name to connect to";

    public static final String POSTGRES_MAPPED_SCHEMA_KEY = "postgresMappedSchema";
    public static final String POSTGRES_MAPPED_SCHEMA_DEFAULT = "";
    public static final String POSTGRES_MAPPED_SCHEMA_DESCRIPTION = "A PostgreSQL schema to map in Virtual Schema";
}