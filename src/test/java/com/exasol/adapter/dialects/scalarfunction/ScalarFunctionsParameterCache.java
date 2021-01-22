package com.exasol.adapter.dialects.scalarfunction;

import java.io.*;
import java.util.*;

import org.yaml.snakeyaml.Yaml;

import com.exasol.errorreporting.ExaError;

/**
 * This class caches the parameters for runs that were passed the tests. By that in upcoming test runs can only test
 * these runs and by that are a lot faster.
 */
public class ScalarFunctionsParameterCache {
    private static final String CACHE_FILE_NAME = "src/test/resources/integration/scalarFunctionsParameterCache.yml";
    private final Map<String, List<String>> parameterCache;
    private final Yaml yaml;

    public ScalarFunctionsParameterCache() {
        try {
            this.yaml = new Yaml();
            this.parameterCache = loadParameterCache();
        } catch (final FileNotFoundException exception) {
            throw new IllegalStateException(ExaError.messageBuilder("E-PGVS-10")
                    .message("Failed to read scalar functions from parameters cache.").toString(), exception);
        }
    }

    private Map<String, List<String>> loadParameterCache() throws FileNotFoundException {
        final Map<String, List<String>> loadedData = this.yaml.load(new FileReader(CACHE_FILE_NAME));
        if (loadedData != null) {
            return new TreeMap<>(loadedData);
        } else {
            return new TreeMap<>();
        }
    }

    synchronized public boolean hasParametersForFunction(final String functionName) {
        return this.parameterCache.containsKey(functionName) && !this.parameterCache.get(functionName).isEmpty();
    }

    synchronized public List<String> getFunctionsValidParameterCombinations(final String functionName) {
        return this.parameterCache.get(functionName);
    }

    synchronized public void setFunctionsValidParameterCombinations(final String functionName,
            final List<String> validParametersCombinations) {
        this.parameterCache.put(functionName, validParametersCombinations);
    }

    synchronized public void removeFunction(final String functionName) {
        this.parameterCache.remove(functionName);
    }

    synchronized public void flush() {
        try (final FileWriter fileWriter = new FileWriter(CACHE_FILE_NAME)) {
            this.yaml.dump(this.parameterCache, fileWriter);
        } catch (final IOException exception) {
            throw new IllegalStateException(
                    ExaError.messageBuilder("E-PGVS-11")
                            .message("Failed to write scalar functions parameter to the cache file.").toString(),
                    exception);
        }
    }
}
