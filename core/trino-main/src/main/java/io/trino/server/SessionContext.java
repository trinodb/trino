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
package io.trino.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.client.ProtocolHeaders;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.session.ResourceEstimates;
import io.trino.spi.tracing.Tracer;
import io.trino.transaction.TransactionId;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class SessionContext
{
    private final ProtocolHeaders protocolHeaders;

    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final Optional<String> path;

    private final Optional<Identity> authenticatedIdentity;
    private final Identity identity;
    private final SelectedRole selectedRole;

    private final Optional<String> source;
    private final Optional<String> traceToken;
    private final Optional<String> userAgent;
    private final Optional<String> remoteUserAddress;
    private final Optional<String> timeZoneId;
    private final Optional<String> language;
    private final Set<String> clientTags;
    private final Set<String> clientCapabilities;
    private final ResourceEstimates resourceEstimates;

    private final Map<String, String> systemProperties;
    private final Map<String, Map<String, String>> catalogSessionProperties;

    private final Map<String, String> preparedStatements;

    private final Optional<TransactionId> transactionId;
    private final boolean clientTransactionSupport;
    private final Optional<String> clientInfo;
    private final Optional<Tracer> tracer;

    public SessionContext(
            ProtocolHeaders protocolHeaders,
            Optional<String> catalog,
            Optional<String> schema,
            Optional<String> path,
            Optional<Identity> authenticatedIdentity,
            Identity identity,
            SelectedRole selectedRole,
            Optional<String> source,
            Optional<String> traceToken,
            Optional<String> userAgent,
            Optional<String> remoteUserAddress,
            Optional<String> timeZoneId,
            Optional<String> language,
            Set<String> clientTags,
            Set<String> clientCapabilities,
            ResourceEstimates resourceEstimates,
            Map<String, String> systemProperties,
            Map<String, Map<String, String>> catalogSessionProperties,
            Map<String, String> preparedStatements,
            Optional<TransactionId> transactionId,
            boolean clientTransactionSupport,
            Optional<String> clientInfo,
            Optional<Tracer> tracer)
    {
        this.protocolHeaders = requireNonNull(protocolHeaders, "protocolHeaders is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.path = requireNonNull(path, "path is null");
        this.authenticatedIdentity = requireNonNull(authenticatedIdentity, "authenticatedIdentity is null");
        this.identity = requireNonNull(identity, "identity is null");
        this.selectedRole = requireNonNull(selectedRole, "selectedRole is null");
        this.source = requireNonNull(source, "source is null");
        this.traceToken = requireNonNull(traceToken, "traceToken is null");
        this.userAgent = requireNonNull(userAgent, "userAgent is null");
        this.remoteUserAddress = requireNonNull(remoteUserAddress, "remoteUserAddress is null");
        this.timeZoneId = requireNonNull(timeZoneId, "timeZoneId is null");
        this.language = requireNonNull(language, "language is null");
        this.clientTags = ImmutableSet.copyOf(requireNonNull(clientTags, "clientTags is null"));
        this.clientCapabilities = ImmutableSet.copyOf(requireNonNull(clientCapabilities, "clientCapabilities is null"));
        this.resourceEstimates = requireNonNull(resourceEstimates, "resourceEstimates is null");
        this.systemProperties = ImmutableMap.copyOf(requireNonNull(systemProperties, "systemProperties is null"));
        requireNonNull(catalogSessionProperties, "catalogSessionProperties is null");
        this.catalogSessionProperties = catalogSessionProperties.entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue())));
        this.preparedStatements = ImmutableMap.copyOf(requireNonNull(preparedStatements, "preparedStatements is null"));
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.clientTransactionSupport = clientTransactionSupport;
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    public ProtocolHeaders getProtocolHeaders()
    {
        return protocolHeaders;
    }

    public Optional<Identity> getAuthenticatedIdentity()
    {
        return authenticatedIdentity;
    }

    public Identity getIdentity()
    {
        return identity;
    }

    public SelectedRole getSelectedRole()
    {
        return selectedRole;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    public Optional<String> getSchema()
    {
        return schema;
    }

    public Optional<String> getPath()
    {
        return path;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public Optional<String> getRemoteUserAddress()
    {
        return remoteUserAddress;
    }

    public Optional<String> getUserAgent()
    {
        return userAgent;
    }

    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    public Set<String> getClientTags()
    {
        return clientTags;
    }

    public Set<String> getClientCapabilities()
    {
        return clientCapabilities;
    }

    public ResourceEstimates getResourceEstimates()
    {
        return resourceEstimates;
    }

    public Optional<String> getTimeZoneId()
    {
        return timeZoneId;
    }

    public Optional<String> getLanguage()
    {
        return language;
    }

    public Optional<Tracer> getTracer()
    {
        return tracer;
    }

    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    public Map<String, Map<String, String>> getCatalogSessionProperties()
    {
        return catalogSessionProperties;
    }

    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    public Optional<TransactionId> getTransactionId()
    {
        return transactionId;
    }

    public boolean supportClientTransaction()
    {
        return clientTransactionSupport;
    }

    public Optional<String> getTraceToken()
    {
        return traceToken;
    }
}
