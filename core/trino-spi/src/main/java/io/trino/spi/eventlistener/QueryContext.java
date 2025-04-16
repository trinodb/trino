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
package io.trino.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Unstable;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.ResourceEstimates;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
public class QueryContext
{
    private final String user;
    private final String originalUser;
    private final Set<String> originalRoles;
    private final Optional<String> principal;
    private final Set<String> enabledRoles;
    private final Set<String> groups;
    private final Optional<String> traceToken;
    private final Optional<String> remoteClientAddress;
    private final Optional<String> userAgent;
    private final Optional<String> clientInfo;
    private final Set<String> clientTags;
    private final Set<String> clientCapabilities;
    private final Optional<String> source;
    private final String timezone;

    private final Optional<String> catalog;
    private final Optional<String> schema;

    private final Optional<ResourceGroupId> resourceGroupId;

    private final Map<String, String> sessionProperties;
    private final ResourceEstimates resourceEstimates;

    private final String serverAddress;
    private final String serverVersion;
    private final String environment;

    private final Optional<QueryType> queryType;

    private final String retryPolicy;

    @JsonCreator
    @Unstable
    public QueryContext(
            String user,
            String originalUser,
            Set<String> originalRoles,
            Optional<String> principal,
            Set<String> enabledRoles,
            Set<String> groups,
            Optional<String> traceToken,
            Optional<String> remoteClientAddress,
            Optional<String> userAgent,
            Optional<String> clientInfo,
            Set<String> clientTags,
            Set<String> clientCapabilities,
            Optional<String> source,
            String timezone,
            Optional<String> catalog,
            Optional<String> schema,
            Optional<ResourceGroupId> resourceGroupId,
            Map<String, String> sessionProperties,
            ResourceEstimates resourceEstimates,
            String serverAddress,
            String serverVersion,
            String environment,
            Optional<QueryType> queryType,
            String retryPolicy)
    {
        this.user = requireNonNull(user, "user is null");
        this.originalUser = requireNonNull(originalUser, "originalUser is null");
        this.originalRoles = requireNonNull(originalRoles, "originalRoles is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.enabledRoles = requireNonNull(enabledRoles, "enabledRoles is null");
        this.groups = requireNonNull(groups, "groups is null");
        this.traceToken = requireNonNull(traceToken, "traceToken is null");
        this.remoteClientAddress = requireNonNull(remoteClientAddress, "remoteClientAddress is null");
        this.userAgent = requireNonNull(userAgent, "userAgent is null");
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.clientTags = requireNonNull(clientTags, "clientTags is null");
        this.clientCapabilities = requireNonNull(clientCapabilities, "clientCapabilities is null");
        this.source = requireNonNull(source, "source is null");
        this.timezone = requireNonNull(timezone, "timezone is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.resourceGroupId = requireNonNull(resourceGroupId, "resourceGroupId is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.resourceEstimates = requireNonNull(resourceEstimates, "resourceEstimates is null");
        this.serverAddress = requireNonNull(serverAddress, "serverAddress is null");
        this.serverVersion = requireNonNull(serverVersion, "serverVersion is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }

    @JsonProperty
    public String getOriginalUser()
    {
        return originalUser;
    }

    @JsonProperty
    public Set<String> getOriginalRoles()
    {
        return originalRoles;
    }

    @JsonProperty
    public Optional<String> getPrincipal()
    {
        return principal;
    }

    @JsonProperty
    public Set<String> getEnabledRoles()
    {
        return enabledRoles;
    }

    @JsonProperty
    public Set<String> getGroups()
    {
        return groups;
    }

    @JsonProperty
    public Optional<String> getTraceToken()
    {
        return traceToken;
    }

    @JsonProperty
    public Optional<String> getRemoteClientAddress()
    {
        return remoteClientAddress;
    }

    @JsonProperty
    public Optional<String> getUserAgent()
    {
        return userAgent;
    }

    @JsonProperty
    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    @JsonProperty
    public Set<String> getClientTags()
    {
        return clientTags;
    }

    @JsonProperty
    public Set<String> getClientCapabilities()
    {
        return clientCapabilities;
    }

    @JsonProperty
    public Optional<String> getSource()
    {
        return source;
    }

    @JsonProperty
    public String getTimezone()
    {
        return timezone;
    }

    @JsonProperty
    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public Optional<String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    public Optional<ResourceGroupId> getResourceGroupId()
    {
        return resourceGroupId;
    }

    @JsonProperty
    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
    }

    @JsonProperty
    public ResourceEstimates getResourceEstimates()
    {
        return resourceEstimates;
    }

    @JsonProperty
    public String getServerAddress()
    {
        return serverAddress;
    }

    @JsonProperty
    public String getServerVersion()
    {
        return serverVersion;
    }

    @JsonProperty
    public String getEnvironment()
    {
        return environment;
    }

    @JsonProperty
    public Optional<QueryType> getQueryType()
    {
        return queryType;
    }

    @JsonProperty
    public String getRetryPolicy()
    {
        return retryPolicy;
    }
}
