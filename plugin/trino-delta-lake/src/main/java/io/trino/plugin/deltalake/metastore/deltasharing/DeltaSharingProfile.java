/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.deltalake.metastore.deltasharing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Represents a Delta Sharing profile file configuration
 */
public class DeltaSharingProfile
{
    private final int shareCredentialsVersion;
    private final String endpoint;
    private final Optional<String> bearerToken;
    private final Optional<String> expirationTime;

    @JsonCreator
    public DeltaSharingProfile(@JsonProperty("shareCredentialsVersion") int shareCredentialsVersion, @JsonProperty("endpoint") String endpoint, @JsonProperty("bearerToken") String bearerToken, @JsonProperty("expirationTime") String expirationTime)
    {
        this.shareCredentialsVersion = shareCredentialsVersion;
        this.endpoint = requireNonNull(endpoint, "endpoint is null");
        this.bearerToken = Optional.ofNullable(bearerToken);
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
    public Optional<String> getBearerToken()
    {
        return bearerToken;
    }

    @JsonProperty
    public Optional<String> getExpirationTime()
    {
        return expirationTime;
    }

    public boolean isExpired()
    {
        return expirationTime.map(expiryString -> {
            try {
                long expiry = Long.parseLong(expiryString);
                return System.currentTimeMillis() / 1000 > expiry;
            }
            catch (NumberFormatException e) {
                return false;
            }
        }).orElse(false);
    }

    public static DeltaSharingProfile fromFile(Path profilePath)
            throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(profilePath.toFile(), DeltaSharingProfile.class);
    }
}
