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
package io.trino.plugin.weaviate;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.weaviate.client6.v1.api.collections.query.ConsistencyLevel;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class WeaviateConfig
{
    private String scheme = "http";
    private String httpHost = "localhost";
    private int httpPort = 8080;
    private String grpcHost = "localhost";
    private int grpcPort = 50051;
    private Duration timeout;
    private ConsistencyLevel consistencyLevel;

    private String apiKey;
    private String accessToken;
    private String refreshToken;
    private Duration accessTokenLifetime;
    private String username;
    private String password;
    private String clientSecret;
    private List<String> scopes = emptyList();

    private OptionalInt pageSize = OptionalInt.empty();

    @NotNull
    public String getScheme()
    {
        return scheme;
    }

    @Config("weaviate.scheme")
    @ConfigDescription("Connection URL scheme (http/https)")
    public WeaviateConfig setScheme(String scheme)
    {
        this.scheme = scheme;
        return this;
    }

    @NotNull
    public String getHttpHost()
    {
        return httpHost;
    }

    @Config("weaviate.http-host")
    public WeaviateConfig setHttpHost(String httpHost)
    {
        this.httpHost = httpHost;
        return this;
    }

    public int getHttpPort()
    {
        return httpPort;
    }

    @Config("weaviate.http-port")
    public WeaviateConfig setHttpPort(int httpPort)
    {
        this.httpPort = httpPort;
        return this;
    }

    @NotNull
    public String getGrpcHost()
    {
        return grpcHost;
    }

    @Config("weaviate.grpc-host")
    public WeaviateConfig setGrpcHost(String grpcHost)
    {
        this.grpcHost = grpcHost;
        return this;
    }

    public int getGrpcPort()
    {
        return grpcPort;
    }

    @Config("weaviate.grpc-port")
    public WeaviateConfig setGrpcPort(int grpcPort)
    {
        this.grpcPort = grpcPort;
        return this;
    }

    @NotNull
    public Optional<Duration> getTimeout()
    {
        return Optional.ofNullable(timeout);
    }

    @Config("weaviate.timeout")
    @ConfigDescription("Query timeout")
    public WeaviateConfig setTimeout(Duration timeout)
    {
        this.timeout = timeout;
        return this;
    }

    @Nullable
    public ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    @Config("weaviate.consistency-level")
    @ConfigDescription("Consistency level for reads and writes")
    public WeaviateConfig setConsistencyLevel(ConsistencyLevel consistencyLevel)
    {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    @Config("weaviate.auth.api-key")
    public WeaviateConfig setApiKey(String apiKey)
    {
        this.apiKey = apiKey;
        return this;
    }

    @NotNull
    public Optional<String> getApiKey()
    {
        return Optional.ofNullable(apiKey);
    }

    @Config("weaviate.auth.access-token")
    @ConfigDescription("Access token for Bearer Token authentication.")
    public WeaviateConfig setAccessToken(String accessToken)
    {
        this.accessToken = accessToken;
        return this;
    }

    @NotNull
    public Optional<String> getAccessToken()
    {
        return Optional.ofNullable(accessToken);
    }

    @Config("weaviate.auth.refresh-token")
    @ConfigDescription("Refresh token for the access token.")
    public WeaviateConfig setRefreshToken(String refreshToken)
    {
        this.refreshToken = refreshToken;
        return this;
    }

    @NotNull
    public Optional<String> getRefreshToken()
    {
        return Optional.ofNullable(refreshToken);
    }

    @Config("weaviate.auth.access-token-lifetime")
    @ConfigDescription("Remaining access token lifetime")
    public WeaviateConfig setAccessTokenLifetime(Duration accessTokenLifetime)
    {
        this.accessTokenLifetime = accessTokenLifetime;
        return this;
    }

    public Optional<Duration> getAccessTokenLifetime()
    {
        return Optional.ofNullable(accessTokenLifetime);
    }

    @Config("weaviate.oidc.username")
    @ConfigDescription("Username for Resource Owner Password authentication flow.")
    public WeaviateConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @NotNull
    public Optional<String> getUsername()
    {
        return Optional.ofNullable(username);
    }

    @Config("weaviate.oidc.password")
    @ConfigDescription("Password for Resource Owner Password authentication flow.")
    public WeaviateConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @NotNull
    public Optional<String> getPassword()
    {
        return Optional.ofNullable(password);
    }

    @Config("weaviate.oidc.client-secret")
    @ConfigDescription("Client secret for Client Credentials authentication flow.")
    public WeaviateConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }

    @NotNull
    public Optional<String> getClientSecret()
    {
        return Optional.ofNullable(clientSecret);
    }

    @Config("weaviate.oidc.scopes")
    @ConfigDescription("Scopes for OIDC authentication flows.")
    public WeaviateConfig setScopes(List<String> scopes)
    {
        this.scopes = ImmutableList.copyOf(requireNonNull(scopes, "scopes is null"));
        return this;
    }

    @NotNull
    public List<String> getScopes()
    {
        return this.scopes;
    }

    @Config("weaviate.page-size")
    @ConfigDescription("Page size for Cursor API pagination.")
    public WeaviateConfig setPageSize(Integer pageSize)
    {
        if (pageSize != null) {
            this.pageSize = OptionalInt.of(pageSize);
        }
        return this;
    }

    public OptionalInt getPageSize()
    {
        return pageSize;
    }
}
