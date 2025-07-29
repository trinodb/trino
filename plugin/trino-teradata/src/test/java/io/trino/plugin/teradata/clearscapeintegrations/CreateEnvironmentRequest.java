package io.trino.plugin.teradata.clearscapeintegrations;

public record CreateEnvironmentRequest(

        String name,

        String region,

        String password
){}
