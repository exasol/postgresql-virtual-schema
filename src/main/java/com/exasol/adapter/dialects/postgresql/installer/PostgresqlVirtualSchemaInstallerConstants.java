package com.exasol.adapter.dialects.postgresql.installer;

public class PostgresqlVirtualSchemaInstallerConstants {
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");
    public static final String CREDENTIALS_FILE_KEY = "credentials_file";
    public static final String CREDENTIALS_FILE_DESCRIPTION = "Path to the file where credentials are stored";
    public static final String CREDENTIALS_FILE_DEFAULT = FILE_SEPARATOR + ".virtual-schema-installer" + FILE_SEPARATOR
            + "credentials";

    // Credentials properties
    public static final String EXASOL_USERNAME_KEY = "exasol_username";
    public static final String EXASOL_PASSWORD_KEY = "exasol_password";
    public static final String EXASOL_BUCKET_WRITE_PASSWORD_KEY = "exasol_bucket_write_password";
    public static final String POSTGRES_USERNAME_KEY = "postgres_username";
    public static final String POSTGRES_PASSWORD_KEY = "postgres_password";

    // User input
    public static final String VIRTUAL_SCHEMA_JAR_NAME_KEY = "virtual-schema-jar-name";
    public static final String VIRTUAL_SCHEMA_JAR_NAME_DEFAULT = "virtual-schema-dist-9.0.1-postgresql-2.0.0.jar";
    public static final String VIRTUAL_SCHEMA_JAR_NAME_DESCRIPTION = "Name of the Virtual Schema JAR file";

    public static final String VIRTUAL_SCHEMA_JAR_PATH_KEY = "virtual-schema-jar-path";
    public static final String VIRTUAL_SCHEMA_JAR_PATH_DEFAULT = "";
    public static final String VIRTUAL_SCHEMA_JAR_PATH_DESCRIPTION = "Path to the Virtual Schema JAR file";

    public static final String JDBC_DRIVER_NAME_KEY = "jdbc-driver-name";
    public static final String JDBC_DRIVER_NAME_DEFAULT = "postgresql.jar";
    public static final String JDBC_DRIVER_NAME_DESCRIPTION = "Name of the PostgreSQL JDBC driver file";

    public static final String JDBC_DRIVER_PATH_KEY = "jdbc-driver-path";
    public static final String JDBC_DRIVER_PATH_DEFAULT = "";
    public static final String JDBC_DRIVER_PATH_DESCRIPTION = "Path to the PostgreSQL JDBC driver file";

    public static final String EXA_IP_KEY = "exa-ip";
    public static final String EXA_IP_DEFAULT = "localhost";
    public static final String EXA_IP_DESCRIPTION = "An IP address to connect to the Exasol database";

    public static final String EXA_PORT_KEY = "exa-port";
    public static final String EXA_PORT_DEFAULT = "8563";
    public static final String EXA_PORT_DESCRIPTION = "A port on which the Exasol database is listening";

    public static final String EXA_BUCKET_FS_PORT_KEY = "exa-bucketfs-port";
    public static final String EXA_BUCKET_FS_PORT_DEFAULT = "2580";
    public static final String EXA_BUCKET_FS_PORT_DESCRIPTION = "A port on which BucketFS is listening";

    public static final String EXA_BUCKET_NAME_KEY = "exa-bucket-name";
    public static final String EXA_BUCKET_NAME_DEFAULT = "default";
    public static final String EXA_BUCKET_NAME_DESCRIPTION = "A bucket name to upload jars";

    public static final String EXA_SCHEMA_NAME_KEY = "exa-schema-name";
    public static final String EXA_SCHEMA_NAME_DEFAULT = "ADAPTER";
    public static final String EXA_SCHEMA_NAME_DESCRIPTION = "A name for an Exasol schema that holds the adapter script";

    public static final String EXA_ADAPTER_NAME_KEY = "exa-adapter-name";
    public static final String EXA_ADAPTER_NAME_DEFAULT = "POSTGRES_ADAPTER_SCRIPT";
    public static final String EXA_ADAPTER_NAME_DESCRIPTION = "A name for an Exasol adapter script";

    public static final String EXA_CONNECTION_NAME_KEY = "exa-connection-name";
    public static final String EXA_CONNECTION_NAME_DEFAULT = "POSTGRES_JDBC_CONNECTION";
    public static final String EXA_CONNECTION_NAME_DESCRIPTION = "A name for an Exasol connection to the Postgres database";

    public static final String EXA_VIRTUAL_SCHEMA_NAME_KEY = "exa-virtual-schema-name";
    public static final String EXA_VIRTUAL_SCHEMA_NAME_DEFAULT = "POSTGRES_VIRTUAL_SCHEMA";
    public static final String EXA_VIRTUAL_SCHEMA_NAME_DESCRIPTION = "A name for a virtual schema";

    public static final String POSTGRES_IP_KEY = "postgres-ip";
    public static final String POSTGRES_IP_DEFAULT = "localhost";
    public static final String POSTGRES_IP_DESCRIPTION = "An IP address to connect to the PostgreSQL database";

    public static final String POSTGRES_PORT_KEY = "postgres-port";
    public static final String POSTGRES_PORT_DEFAULT = "5432";
    public static final String POSTGRES_PORT_DESCRIPTION = "A port on which the PostgreSQL database is listening";

    public static final String POSTGRES_DATABASE_NAME_KEY = "postgres-database-name";
    public static final String POSTGRES_DATABASE_NAME_DEFAULT = "postgres";
    public static final String POSTGRES_DATABASE_NAME_DESCRIPTION = "A PostgreSQL database name to connect to";

    public static final String POSTGRES_MAPPED_SCHEMA_KEY = "postgres-mapped-schema";
    public static final String POSTGRES_MAPPED_SCHEMA_DEFAULT = "";
    public static final String POSTGRES_MAPPED_SCHEMA_DESCRIPTION = "A PostgreSQL schema to map in Virtual Schema";

    public static final String ADDITIONAL_PROPERTY_KEY = "property";
    public static final String HELP_KEY = "property";
}