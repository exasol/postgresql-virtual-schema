package com.exasol.adapter.dialects.postgresql.installer;

import static com.exasol.adapter.dialects.postgresql.installer.PostgresqlVirtualSchemaInstallerConstants.ADDITIONAL_PROPERTY_KEY;
import static com.exasol.adapter.dialects.postgresql.installer.PostgresqlVirtualSchemaInstallerConstants.HELP_KEY;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.*;

/**
 * Parses user input for the {@link Installer} class.
 */
public class UserInputParser {
    public UserInput parseUserInput(final String[] args, final Map<String, String> optionsMap)
            throws ParseException {
        final Options options = createOptions(optionsMap);
        final CommandLine cmd = getCommandLine(args, options);
        printHelpIfNeeded(options, cmd);
        final Map<String, String> userInput = new HashMap<>();
        for (final String option : optionsMap.keySet()) {
            userInput.put(option, cmd.getOptionValue(option));
        }
        final String[] additionalProperties = cmd.getOptionValues(ADDITIONAL_PROPERTY_KEY);
        return new UserInput(userInput, additionalProperties);
    }

    private void printHelpIfNeeded(final Options options, final CommandLine cmd) {
        if (cmd.hasOption(HELP_KEY)) {
            printHelp(options);
            System.exit(0);
        }
    }

    private Options createOptions(final Map<String, String> optionsMap) {
        final Options options = new Options();
        options.addOption(new Option(HELP_KEY, HELP_KEY, false, "Help command"));
        for (final Map.Entry<String, String> entry : optionsMap.entrySet()) {
            options.addOption(new Option(null, entry.getKey(), true, entry.getValue()));
        }
        final Option property = new Option(ADDITIONAL_PROPERTY_KEY, ADDITIONAL_PROPERTY_KEY, true,
                "Additional virtual schema property.");
        property.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(property);
        return options;
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