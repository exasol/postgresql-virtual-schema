package com.exasol.adapter.dialects.postgresql.installer;

/**
 * Represents a user with username and password.
 */
public class Credentials {
    private final String username;
    private final String password;

    /**
     * Create new {@link User}.
     *
     * @param username username as a string
     * @param password password as a string
     */
    public User(final String username, final String password) {
        this.username = username;
        this.password = password;
    }

    /**
     * Get a username.
     *
     * @return username
     */
    public String getUsername() {
        return this.username;
    }

    /**
     * Get a password.
     *
     * @return password
     */
    public String getPassword() {
        return this.password;
    }
}
