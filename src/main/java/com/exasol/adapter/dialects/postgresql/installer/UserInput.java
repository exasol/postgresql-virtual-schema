package com.exasol.adapter.dialects.postgresql.installer;

import java.util.Map;

import lombok.Getter;

@Getter
public class UserInput {
    private final String[] additionalProperties;
    private final Map<String, String> parameters;

    public UserInput(final Map<String, String> parameters, final String[] additionalProperties) {
        this.parameters = parameters;
        this.additionalProperties = additionalProperties;
    }
}