package io.trino.plugin.teradata.clearscape;

public record EnvironmentRequest(

        String name,

        OperationRequest request
) {}
