package io.trino.plugin.teradata.clearScapeIntegrations;

public record EnvironmentRequest(

        String name,

        OperationRequest request

) {}
