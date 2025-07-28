package io.trino.plugin.teradata.clearScapeIntegrations;

public record CreateEnvironmentRequest(

        String name,

        String region,

        String password

) {

}
