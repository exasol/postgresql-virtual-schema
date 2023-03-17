package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.AdapterProperties.IGNORE_ERRORS_PROPERTY;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.properties.PropertyValidationException;
import com.exasol.adapter.properties.PropertyValidator;
import com.exasol.errorreporting.ExaError;

/**
 * This enumeration defines the behavior of PostgresSQL when it comes to dealing with unquoted identifiers (e.g. table
 * names).
 */
public class PostgreSQLIdentifierMapping {

    /** Name of adapter property controlling identifier mapping **/
    public static final String PROPERTY = "POSTGRESQL_IDENTIFIER_MAPPING";
    /** Name of switch for upper case table names **/
    public static final String UPPERCASE_TABLES_SWITCH = "POSTGRESQL_UPPERCASE_TABLES";

    enum CaseFolding {
        CONVERT_TO_UPPER("CONVERT_TO_UPPER"), //
        PRESERVE_ORIGINAL_CASE("PRESERVE_ORIGINAL_CASE");

        public static final CaseFolding DEFAULT = CONVERT_TO_UPPER;

        String representation;

        private CaseFolding(final String string) {
            this.representation = string;
        }

        static String validValues() {
            return Stream.of(values()).map(c -> c.representation).collect(Collectors.joining(","));
        }

        static Optional<CaseFolding> map(final String name) {
            return Stream.of(CaseFolding.values()) //
                    .filter(c -> c.representation.equals(name)) //
                    .findAny();
        }
    }

    /**
     * Parse the identifier mapping from a string.
     *
     * @param name string describing the mapping
     * @return PosgreSQL identifier mapping
     * @throws IllegalArgumentException if the given string contains a mapping name that is unknown or
     *                                  <code>null</code>.
     */
    public static CaseFolding parse(final String name) {
        if (name == null) {
            throw new IllegalArgumentException(ExaError.messageBuilder("E-VSPG-1")
                    .message("Unable to parse PostgreSQL identifier mapping from a null value.").toString());
        }
        return CaseFolding.map(name).orElseThrow( //
                () -> new IllegalArgumentException(ExaError.messageBuilder("E-VSPG-2") //
                        .message("Unable to parse PostgreSQL identifier mapping {{mapping}}.", name) //
                        .toString()));
    }

    /**
     * Read identifier mapping from adapter properties.
     *
     * @param properties adapter properties to read identifier mapping from
     * @return identifier mapping from properties or default value
     */
    public static CaseFolding from(final AdapterProperties properties) {
        return properties.containsKey(PROPERTY) //
                ? CaseFolding.valueOf(properties.get(PROPERTY))
                : CaseFolding.DEFAULT;
    }

    /**
     * @return validator for adapter properties controlling identifier mapping
     */
    public static PropertyValidator validator() {
        return new Validator();
    }

    private static class Validator implements PropertyValidator {
        @Override
        public void validate(final AdapterProperties properties) throws PropertyValidationException {
            if (properties.containsKey(PROPERTY)) {
                if (CaseFolding.map(properties.get(PROPERTY)).isEmpty()) {
                    throw new PropertyValidationException(ExaError.messageBuilder("E-VSPG-4") //
                            .message("Value for {{property}} must be one of [{{values}}].", PROPERTY,
                                    CaseFolding.validValues()) //
                            .toString());
                }
            }
            if (properties.hasIgnoreErrors()
                    && !List.of(UPPERCASE_TABLES_SWITCH).containsAll(properties.getIgnoredErrors())) {
                throw new PropertyValidationException(ExaError.messageBuilder("E-VSPG-5") //
                        .message("Unknown error identifier in list of ignored errors ({{propertyName}}).",
                                IGNORE_ERRORS_PROPERTY)
                        .mitigation("Pick one of: {{availableValues}}", UPPERCASE_TABLES_SWITCH).toString());
            }
        }
    }
}
