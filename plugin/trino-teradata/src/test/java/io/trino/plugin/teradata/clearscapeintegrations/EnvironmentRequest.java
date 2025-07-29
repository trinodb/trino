package io.trino.plugin.teradata.clearscapeintegrations;

public record EnvironmentRequest(

        String name,

        OperationRequest request
) {}
