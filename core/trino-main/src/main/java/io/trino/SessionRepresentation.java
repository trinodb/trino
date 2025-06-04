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
package io.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.opentelemetry.api.trace.Span;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.QueryId;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.session.ResourceEstimates;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.SqlPath;
import io.trino.transaction.TransactionId;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static io.trino.client.ProtocolHeaders.createProtocolHeaders;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public final class SessionRepresentation
{
    private final String queryId;
    private final Span querySpan;
    private final Optional<TransactionId> transactionId;
    private final boolean clientTransactionSupport;
    private final String user;
    private final String originalUser;
    private final Set<String> originalRoles;
    private final Set<String> groups;
    private final Set<String> originalUserGroups;
    private final Optional<String> principal;
    private final Set<String> enabledRoles;
    private final Optional<String> source;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final SqlPath path;
    private final Optional<String> traceToken;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    private final Optional<String> remoteUserAddress;
    private final Optional<String> userAgent;
    private final Optional<String> clientInfo;
    private final Set<String> clientTags;
    private final Set<String> clientCapabilities;
    private final Instant start;
    private final ResourceEstimates resourceEstimates;
    private final Map<String, String> systemProperties;
    private final Map<String, Map<String, String>> catalogProperties;
    private final Map<String, SelectedRole> catalogRoles;
    private final Map<String, String> preparedStatements;
    private final String protocolName;
    private final Optional<String> queryDataEncoding;

    @JsonCreator
    public SessionRepresentation(
            @JsonProperty("queryId") String queryId,
            @JsonProperty("querySpan") Span querySpan,
            @JsonProperty("transactionId") Optional<TransactionId> transactionId,
            @JsonProperty("clientTransactionSupport") boolean clientTransactionSupport,
            @JsonProperty("user") String user,
            @JsonProperty("originalUser") String originalUser,
            @JsonProperty("setOriginalRoles") Set<String> originalRoles,
            @JsonProperty("groups") Set<String> groups,
            @JsonProperty("originalUserGroups") Set<String> originalUserGroups,
            @JsonProperty("principal") Optional<String> principal,
            @JsonProperty("enabledRoles") Set<String> enabledRoles,
            @JsonProperty("source") Optional<String> source,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("path") SqlPath path,
            @JsonProperty("traceToken") Optional<String> traceToken,
            @JsonProperty("timeZoneKey") TimeZoneKey timeZoneKey,
            @JsonProperty("locale") Locale locale,
            @JsonProperty("remoteUserAddress") Optional<String> remoteUserAddress,
            @JsonProperty("userAgent") Optional<String> userAgent,
            @JsonProperty("clientInfo") Optional<String> clientInfo,
            @JsonProperty("clientTags") Set<String> clientTags,
            @JsonProperty("clientCapabilities") Set<String> clientCapabilities,
            @JsonProperty("resourceEstimates") ResourceEstimates resourceEstimates,
            @JsonProperty("start") Instant start,
            @JsonProperty("systemProperties") Map<String, String> systemProperties,
            @JsonProperty("catalogProperties") Map<String, Map<String, String>> catalogProperties,
            @JsonProperty("catalogRoles") Map<String, SelectedRole> catalogRoles,
            @JsonProperty("preparedStatements") Map<String, String> preparedStatements,
            @JsonProperty("protocolName") String protocolName,
            @JsonProperty("queryDataEncoding") Optional<String> queryDataEncoding)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.querySpan = requireNonNull(querySpan, "querySpan is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.clientTransactionSupport = clientTransactionSupport;
        this.user = requireNonNull(user, "user is null");
        this.originalUser = requireNonNull(originalUser, "originalUser is null");
        this.originalRoles = requireNonNull(originalRoles, "setOriginalRoles is null");
        this.groups = requireNonNull(groups, "groups is null");
        this.originalUserGroups = requireNonNull(originalUserGroups, "originalUserGroups is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.enabledRoles = ImmutableSet.copyOf(requireNonNull(enabledRoles, "enabledRoles is null"));
        this.source = requireNonNull(source, "source is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.path = requireNonNull(path, "path is null");
        this.traceToken = requireNonNull(traceToken, "traceToken is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.locale = requireNonNull(locale, "locale is null");
        this.remoteUserAddress = requireNonNull(remoteUserAddress, "remoteUserAddress is null");
        this.userAgent = requireNonNull(userAgent, "userAgent is null");
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.clientTags = requireNonNull(clientTags, "clientTags is null");
        this.clientCapabilities = requireNonNull(clientCapabilities, "clientCapabilities is null");
        this.resourceEstimates = requireNonNull(resourceEstimates, "resourceEstimates is null");
        this.start = start;
        this.systemProperties = ImmutableMap.copyOf(systemProperties);
        this.catalogRoles = ImmutableMap.copyOf(catalogRoles);
        this.preparedStatements = ImmutableMap.copyOf(preparedStatements);
        this.protocolName = requireNonNull(protocolName, "protocolName is null");
        this.queryDataEncoding = requireNonNull(queryDataEncoding, "queryDataEncoding is null");

        ImmutableMap.Builder<String, Map<String, String>> catalogPropertiesBuilder = ImmutableMap.builder();
        for (Entry<String, Map<String, String>> entry : catalogProperties.entrySet()) {
            catalogPropertiesBuilder.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
        }
        this.catalogProperties = catalogPropertiesBuilder.buildOrThrow();
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public Span getQuerySpan()
    {
        return querySpan;
    }

    @JsonProperty
    public Optional<TransactionId> getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public boolean isClientTransactionSupport()
    {
        return clientTransactionSupport;
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
    public Set<String> getGroups()
    {
        return groups;
    }

    @JsonProperty
    public Set<String> getOriginalUserGroups()
    {
        return originalUserGroups;
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
    public Optional<String> getSource()
    {
        return source;
    }

    @JsonProperty
    public Optional<String> getTraceToken()
    {
        return traceToken;
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
    public SqlPath getPath()
    {
        return path;
    }

    @JsonProperty
    public TimeZoneKey getTimeZoneKey()
    {
        return timeZoneKey;
    }

    @JsonProperty
    public Locale getLocale()
    {
        return locale;
    }

    @JsonProperty
    public Optional<String> getRemoteUserAddress()
    {
        return remoteUserAddress;
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
    public Instant getStart()
    {
        return start;
    }

    @JsonProperty
    public ResourceEstimates getResourceEstimates()
    {
        return resourceEstimates;
    }

    @JsonProperty
    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    @JsonProperty
    public Map<String, Map<String, String>> getCatalogProperties()
    {
        return catalogProperties;
    }

    @JsonProperty
    public Map<String, SelectedRole> getCatalogRoles()
    {
        return catalogRoles;
    }

    @JsonProperty
    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    @JsonProperty
    public String getProtocolName()
    {
        return protocolName;
    }

    @JsonProperty
    public String getTimeZone()
    {
        return timeZoneKey.getId();
    }

    @JsonProperty
    public Optional<String> getQueryDataEncoding()
    {
        return queryDataEncoding;
    }

    public Identity toIdentity()
    {
        return toIdentity(emptyMap());
    }

    public Identity toIdentity(Map<String, String> extraCredentials)
    {
        return Identity.forUser(user)
                .withGroups(groups)
                .withPrincipal(principal.map(BasicPrincipal::new))
                .withEnabledRoles(enabledRoles)
                .withConnectorRoles(catalogRoles)
                .withExtraCredentials(extraCredentials)
                .build();
    }

    public Identity toOriginalIdentity(Map<String, String> extraCredentials)
    {
        return Identity.forUser(originalUser)
                .withGroups(originalUserGroups)
                .withPrincipal(principal.map(BasicPrincipal::new))
                .withEnabledRoles(originalRoles)
                .withExtraCredentials(extraCredentials)
                .build();
    }

    public Session toSession(SessionPropertyManager sessionPropertyManager)
    {
        return toSession(sessionPropertyManager, emptyMap(), Optional.empty());
    }

    public Session toSession(SessionPropertyManager sessionPropertyManager, Map<String, String> extraCredentials, Optional<Slice> exchangeEncryptionKey)
    {
        return new Session(
                new QueryId(queryId),
                querySpan,
                transactionId,
                clientTransactionSupport,
                toIdentity(extraCredentials),
                toOriginalIdentity(extraCredentials),
                source,
                catalog,
                schema,
                path,
                traceToken,
                timeZoneKey,
                locale,
                remoteUserAddress,
                userAgent,
                clientInfo,
                clientTags,
                clientCapabilities,
                resourceEstimates,
                start,
                systemProperties,
                catalogProperties,
                sessionPropertyManager,
                preparedStatements,
                createProtocolHeaders(protocolName),
                exchangeEncryptionKey,
                queryDataEncoding);
    }
}
