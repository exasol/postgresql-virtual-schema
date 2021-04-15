package com.exasol.adapter.dialects.postgresql.installer;

import java.io.Console;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * This class provides user credentials for different platforms.
 */
public final class CredentialsProvider {
    private static final Logger LOGGER = Logger.getLogger(CredentialsProvider.class.getName());
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    private static final String CREDENTIALS_FILE = FILE_SEPARATOR + ".virtual-schema-installer" + FILE_SEPARATOR
            + "credentials";
    private static final String EXASOL_USERNAME_KEY = "exasol_username";
    private static final String EXASOL_PASSWORD_KEY = "exasol_password";
    private static final String EXASOL_BUCKET_WRITE_PASSWORD_KEY = "exasol_bucket_write_password";
    private static final String POSTGRES_USERNAME_KEY = "postgres_username";
    private static final String POSTGRES_PASSWORD_KEY = "postgres_password";
    private static CredentialsProvider credentialsProvider;

    private CredentialsProvider() {
        // prevents instantiation
    }

    /**
     * Get an instance of {@link CredentialsProvider}.
     *
     * @return instance of {@link CredentialsProvider}
     */
    public static CredentialsProvider getInstance() {
        if (credentialsProvider == null) {
            credentialsProvider = new CredentialsProvider();
        }
        return credentialsProvider;
    }

    /**
     * Get Exasol credentials.
     *
     * @return new instance of {@link User}
     */
    public User provideExasolUser() {
        return createUserWithUserNameAndPassword(EXASOL_USERNAME_KEY, EXASOL_PASSWORD_KEY);
    }

    private User createUserWithUserNameAndPassword(final String usernameKey, final String usernamePassword) {
        final Map<String, String> credentials = getCredentials(usernameKey, usernamePassword);
        final String username = credentials.get(usernameKey);
        final String token = credentials.get(usernamePassword);
        return new User(username, token);
    }

    private Map<String, String> getCredentials(final String... mapKeys) {
        final Optional<Map<String, String>> properties = getCredentialsFromFile(mapKeys);
        if (properties.isPresent()) {
            LOGGER.fine(() -> "Using credentials from file.");
            return properties.get();
        } else {
            LOGGER.fine(() -> "Credentials are not found in the file.");
            return getCredentialsFromConsole(mapKeys);
        }
    }

    private Optional<Map<String, String>> getCredentialsFromFile(final String... mapKeys) {
        LOGGER.fine(() -> "Retrieving credentials from the file '" + CREDENTIALS_FILE + "'.");
        final String homeDirectory = System.getProperty("user.home");
        final String credentialsPath = homeDirectory + CREDENTIALS_FILE;
        return readCredentialsFromFile(credentialsPath, mapKeys);
    }

    private Optional<Map<String, String>> readCredentialsFromFile(final String credentialsPath,
            final String... mapKeys) {
        try (final InputStream stream = new FileInputStream(credentialsPath)) {
            final Properties properties = new Properties();
            properties.load(stream);
            final Map<String, String> propertiesMap = new HashMap<>();
            for (final String key : mapKeys) {
                final String value = properties.getProperty(key);
                if (value == null) {
                    return Optional.empty();
                } else {
                    propertiesMap.put(key, value);
                }
            }
            return Optional.of(propertiesMap);
        } catch (final IOException exception) {
            return Optional.empty();
        }
    }

    private Map<String, String> getCredentialsFromConsole(final String... mapKeys) {
        final Console console = System.console();
        final Map<String, String> credentials = new HashMap<>();
        for (final String key : mapKeys) {
            final String value = console.readLine("Enter " + key.replace("_", " "));
            credentials.put(key, value);
        }
        return credentials;
    }

    /**
     * Get Postgres credentials.
     *
     * @return new instance of {@link User}
     */
    public User providePostgresUser() {
        return createUserWithUserNameAndPassword(POSTGRES_USERNAME_KEY, POSTGRES_PASSWORD_KEY);
    }

    /**
     * Get bucket credentials.
     *
     * @return new instance of {@link User}
     */
    public User provideBucketUser() {
        final Map<String, String> credentials = getCredentials(EXASOL_BUCKET_WRITE_PASSWORD_KEY);
        final String token = credentials.get(EXASOL_BUCKET_WRITE_PASSWORD_KEY);
        return new User("", token);
    }
}