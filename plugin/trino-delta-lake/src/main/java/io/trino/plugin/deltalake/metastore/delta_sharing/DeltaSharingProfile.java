package io.trino.plugin.deltalake.metastore.delta_sharing;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public class DeltaSharingProfile {
    private final int shareCredentialsVersion;
    private final String endpoint;
    private final String bearerToken;
    private final Optional<String> expirationTime;

    @JsonCreator
    public DeltaSharingProfile(
            @JsonProperty("shareCredentialsVersion") int shareCredentialsVersion,
            @JsonProperty("endpoint") String endpoint,
            @JsonProperty("bearerToken") String bearerToken,
            @JsonProperty("expirationTime") String expirationTime)
    {
        this.shareCredentialsVersion = shareCredentialsVersion;
        this.endpoint = endpoint;
        this.bearerToken = bearerToken;
        this.expirationTime = Optional.ofNullable(expirationTime);
    }

    @JsonProperty
    public int getShareCredentialsVersion()
    {
        return shareCredentialsVersion;
    }

    @JsonProperty
    public String getEndpoint()
    {
        return endpoint;
    }

    @JsonProperty
    public String getBearerToken()
    {
        return bearerToken;
    }

    @JsonProperty
    public Optional<String> getExpirationTime()
    {
        return expirationTime;
    }

    public static DeltaSharingProfile fromFile(Path profilePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(profilePath.toFile(), DeltaSharingProfile.class);
    }
}
