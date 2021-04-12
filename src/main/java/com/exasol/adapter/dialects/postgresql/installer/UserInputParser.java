package com.exasol.adapter.dialects.postgresql.installer;

import org.apache.commons.cli.*;

public class UserInputParser {
    public Installer parseUserInput(final String[] args) throws ParseException {
        final Options options = createOptions();
        final CommandLine cmd = getCommandLine(args, options);
        printHelpIfNeeded(options, cmd);
        return Installer.builder() //
                .exasolIpAddress(cmd.getOptionValue("exasolIpAddress")) //
                .exasolBucketFsPort(cmd.getOptionValue("exasolBucketFsPort")) //
                .exasolDatabasePort(cmd.getOptionValue("exasolDatabasePort")) //
                .bucketName(cmd.getOptionValue("bucketName")) //
                .bucketWritePassword(cmd.getOptionValue("bucketWritePassword")) //
                .exasolUser(cmd.getOptionValue("exasolUser")) //
                .exasolPassword(cmd.getOptionValue("exasolPassword")) //
                .postgresIpAddress(cmd.getOptionValue("postgresIpAddress")) //
                .postgresPort(cmd.getOptionValue("postgresPort")) //
                .postgresUsername(cmd.getOptionValue("postgresUsername")) //
                .postgresPassword(cmd.getOptionValue("postgresPassword")) //
                .postgresDatabaseName(cmd.getOptionValue("postgresDatabaseName")) //
                .postgresMappedSchema(cmd.getOptionValue("postgresMappedSchema")) //
                .virtualSchemaName(cmd.getOptionValue("virtualSchemaName")) //
                .build();
    }

    private void printHelpIfNeeded(final Options options, final CommandLine cmd) {
        if (cmd.hasOption("help")) {
            printHelp(options);
            System.exit(0);
        }
    }

    private static Options createOptions() {
        final Option help = new Option("help", false, "Help command");
        final Option exasolIpAddress = new Option("exasolIpAddress", true,
                "IP address to connect to Exasol database (default: localhost).");
        final Option exasolBucketFsPort = new Option("exasolBucketFsPort", true,
                "A port on which BucketFS is listening (default: 2580).");
        final Option exasolDatabasePort = new Option("exasolDatabasePort", true,
                "A port on which Exasol database is listening (default: 8563).");
        final Option bucketName = new Option("bucketName", true, "A bucket name to upload jars (default: default).");
        final Option bucketWritePassword = new Option("bucketWritePassword", true,
                "A password to write to the bucket (default: write)");
        final Option exasolUser = new Option("exasolUser", true, "Exasol user (default: sys).");
        final Option exasolPassword = new Option("exasolPassword", true, "Exasol password (default: exasol).");
        final Option postgresIpAddress = new Option("postgresIpAddress", true,
                "IP address to connect to Postgres database (default: localhost).");
        final Option postgresPort = new Option("postgresPort", true,
                "A port on which Postgres database is listening (default: 5432).");
        final Option postgresUsername = new Option("postgresUsername", true, "Postgres username (default: postgres).");
        final Option postgresPassword = new Option("postgresPassword", true, "Postgres password (default: admin).");
        final Option postgresDatabaseName = new Option("postgresDatabaseName", true,
                "Postgres database name (default: postgres).");
        final Option postgresMappedSchema = new Option("postgresMappedSchema", true,
                "Postgres schema to map in Virtual Schema.");
        final Option virtualSchemaName = new Option("virtualSchemaName", true,
                "Name for a virtual schema (default: POSTGRES_VIRTUAL_SCHEMA).");
        return new Options().addOption(help).addOption(exasolIpAddress).addOption(exasolBucketFsPort)
                .addOption(exasolDatabasePort).addOption(bucketName).addOption(bucketWritePassword)
                .addOption(exasolUser).addOption(exasolPassword).addOption(postgresIpAddress).addOption(postgresPort)
                .addOption(postgresUsername).addOption(postgresPassword).addOption(postgresDatabaseName)
                .addOption(postgresMappedSchema).addOption(virtualSchemaName);
    }

    private static CommandLine getCommandLine(final String[] args, final Options options) throws ParseException {
        try {
            final CommandLineParser parser = new DefaultParser();
            return parser.parse(options, args);
        } catch (final ParseException exception) {
            printHelp(options);
            throw exception;
        }
    }

    private static void printHelp(final Options options) {
        final HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Postgres Virtual Schema Installer", options);
    }
}
