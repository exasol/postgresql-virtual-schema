package com.exasol.adapter.dialects.scalarfunction;

import static com.exasol.adapter.capabilities.ScalarFunctionCapability.MIN_SCALE;

import java.util.Map;

import com.exasol.adapter.capabilities.ScalarFunctionCapability;

/**
 * This class builds scalar function calls.
 */
public class ScalarFunctionCallBuilder {
    /**
     * Some functions have different names than their capabilities. For that we define a mapping here. If a function is
     * not listed, we assume, it has the same name as it's capability.
     */
    private static final Map<ScalarFunctionCapability, String> RENAME_FUNCTIONS = Map.of(MIN_SCALE, "MIN");

    private String getFunctionName(final ScalarFunctionCapability function) {
        return RENAME_FUNCTIONS.getOrDefault(function, function.name());
    }

    public String buildFunctionCall(final ScalarFunctionCapability function, final String parameters) {
        return getFunctionName(function) + "(" + String.join(", ", parameters) + ")";
    }
}
