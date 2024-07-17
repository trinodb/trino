package io.trino.loki;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;

public class LokiConnectorConfig {
    private URI lokiURI = URI.create("http://localhost:3100");
    private String user;
    private String password;

    @NotNull
    public URI getLokiURI()
    {
        return lokiURI;
    }

    @Config("loki.uri")
    @ConfigDescription("Loki query endpoint") // TODO: decide if with or without /loki/api/v1/query
    public LokiConnectorConfig setPrometheusURI(URI lokiURI)
    {
        this.lokiURI = lokiURI;
        return this;
    }

    @NotNull
    public Optional<String> getUser()
    {
        return Optional.ofNullable(user);
    }

    @Config("loki.auth.user")
    public LokiConnectorConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    @NotNull
    public Optional<String> getPassword()
    {
        return Optional.ofNullable(password);
    }

    @Config("loki.auth.password")
    @ConfigSecuritySensitive
    public LokiConnectorConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }
}
