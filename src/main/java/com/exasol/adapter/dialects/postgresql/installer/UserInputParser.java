package com.exasol.adapter.dialects.postgresql.installer;

import org.apache.commons.cli.*;

/**
 * Parses user input for the {@link Installer} class.
 */
public class UserInputParser {
    public Installer parseUserInput(final String[] args, final User exaUser, final User postgresUser) throws ParseException {
        final Options options = createOptions();
        final CommandLine cmd = getCommandLine(args, options);
        printHelpIfNeeded(options, cmd);
        return Installer.builder(exaUser, postgresUser) //
                .virtualSchemaJarName(cmd.getOptionValue("virtualSchemaJarName")) //
                .virtualSchemaJarPath(cmd.getOptionValue("virtualSchemaJarPath")) //
                .jdbcDriverName(cmd.getOptionValue("jdbcDriverName")) //
                .jdbcDriverPath(cmd.getOptionValue("jdbcDriverPath")) //
                .exaIp(cmd.getOptionValue("exaIp")) //
                .exaPort(cmd.getOptionValue("exaPort")) //
                .exaBucketFsPort(cmd.getOptionValue("exaBucketFsPort")) //
                .exaBucketName(cmd.getOptionValue("exaBucketName")) //
                .exaBucketWritePassword(cmd.getOptionValue("exaBucketWritePassword")) //
                .exaSchemaName(cmd.getOptionValue("exaSchemaName")) //
                .exaAdapterName(cmd.getOptionValue("exaAdapterName")) //
                .exaConnectionName(cmd.getOptionValue("exaConnectionName")) //
                .exaVirtualSchemaName(cmd.getOptionValue("exaVirtualSchemaName")) //
                .postgresIp(cmd.getOptionValue("postgresIp")) //
                .postgresPort(cmd.getOptionValue("postgresPort")) //
                .postgresDatabaseName(cmd.getOptionValue("postgresDatabaseName")) //
                .postgresMappedSchema(cmd.getOptionValue("postgresMappedSchema")) //
                .build();
    }

    private void printHelpIfNeeded(final Options options, final CommandLine cmd) {
        if (cmd.hasOption("help")) {
            printHelp(options);
            System.exit(0);
        }
    }

    private Options createOptions() {
        final Options options = new Options();
        addOption(options, "help", false, "Help command");

        addOption(options, "virtualSchemaJarName", true,
                "Name of the Virtual Schema JAR file (default: virtual-schema-dist-9.0.1-postgresql-2.0.0.jar).");
        addOption(options, "virtualSchemaJarPath", true,
                "Path to the Virtual Schema JAR file (default: current directory).");
        addOption(options, "jdbcDriverName", true,
                "Name of the PostgreSQL JDBC driver file (default: postgresql.jar).");
        addOption(options, "jdbcDriverPath", true,
                "Path to the PostgreSQL JDBC driver file (default: current directory).");

        addOption(options, "exaIp", true, "An IP address to connect to the Exasol database (default: localhost).");
        addOption(options, "exaPort", true, "A port on which the Exasol database is listening (default: 8563).");
        addOption(options, "exaBucketFsPort", true, "A port on which BucketFS is listening (default: 2580).");
        addOption(options, "exaBucketName", true, "A bucket name to upload jars (default: default).");
        addOption(options, "exaBucketWritePassword", true, "A password to write to the bucket (default: write)");
        addOption(options, "exaSchemaName", true,
                "A name for an Exasol schema that holds the adapter script (default: ADAPTER).");
        addOption(options, "exaAdapterName", true,
                "A name for an Exasol adapter script (default: POSTGRES_ADAPTER_SCRIPT).");
        addOption(options, "exaConnectionName", true,
                "A name for an Exasol connection to the Postgres database (default: POSTGRES_JDBC_CONNECTION).");
        addOption(options, "exaVirtualSchemaName", true,
                "A name for a virtual schema (default: POSTGRES_VIRTUAL_SCHEMA).");

        addOption(options, "postgresIp", true,
                "An IP address to connect to the PostgreSQL database (default: localhost).");
        addOption(options, "postgresPort", true,
                "A port on which the PostgreSQL database is listening (default: 5432).");
        addOption(options, "postgresDatabaseName", true,
                "A PostgreSQL database name to connect to (default: postgres).");
        addOption(options, "postgresMappedSchema", true,
                "A PostgreSQL schema to map in Virtual Schema (no default value).");
        return options;
    }

    private void addOption(final Options options, final String opt, final boolean hasArg, final String description) {
        final Option option = new Option(opt, hasArg, description);
        options.addOption(option);
    }

    private CommandLine getCommandLine(final String[] args, final Options options) throws ParseException {
        try {
            final CommandLineParser parser = new DefaultParser();
            return parser.parse(options, args);
        } catch (final ParseException exception) {
            printHelp(options);
            throw exception;
        }
    }

    private void printHelp(final Options options) {
        final HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Postgres Virtual Schema Installer", options);
    }
}