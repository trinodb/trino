package io.trino.plugin.teradata.clearscape;

public record CreateEnvironmentRequest(

        String name,

        String region,

        String password
) {}
