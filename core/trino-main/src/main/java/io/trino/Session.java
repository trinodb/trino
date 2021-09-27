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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.client.ProtocolHeaders;
import io.trino.connector.CatalogName;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.session.ResourceEstimates;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.SqlPath;
import io.trino.sql.tree.Execute;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;

import java.security.Principal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.connector.CatalogName.createInformationSchemaCatalogName;
import static io.trino.connector.CatalogName.createSystemTablesCatalogName;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.util.Failures.checkCondition;
import static java.util.Objects.requireNonNull;

public final class Session
{
    private final QueryId queryId;
    private final Optional<TransactionId> transactionId;
    private final boolean clientTransactionSupport;
    private final Identity identity;
    private final Optional<String> source;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final SqlPath path;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    private final Optional<String> remoteUserAddress;
    private final Optional<String> userAgent;
    private final Optional<String> clientInfo;
    private final Optional<String> traceToken;
    private final Set<String> clientTags;
    private final Set<String> clientCapabilities;
    private final ResourceEstimates resourceEstimates;
    private final Instant start;
    private final Map<String, String> systemProperties;
    // TODO use Table
    private final Map<CatalogName, Map<String, String>> connectorProperties;
    // TODO use Table
    private final Map<String, Map<String, String>> unprocessedCatalogProperties;
    private final SessionPropertyManager sessionPropertyManager;
    private final Map<String, String> preparedStatements;
    private final ProtocolHeaders protocolHeaders;

    public Session(
            QueryId queryId,
            Optional<TransactionId> transactionId,
            boolean clientTransactionSupport,
            Identity identity,
            Optional<String> source,
            Optional<String> catalog,
            Optional<String> schema,
            SqlPath path,
            Optional<String> traceToken,
            TimeZoneKey timeZoneKey,
            Locale locale,
            Optional<String> remoteUserAddress,
            Optional<String> userAgent,
            Optional<String> clientInfo,
            Set<String> clientTags,
            Set<String> clientCapabilities,
            ResourceEstimates resourceEstimates,
            Instant start,
            Map<String, String> systemProperties,
            Map<CatalogName, Map<String, String>> connectorProperties,
            Map<String, Map<String, String>> unprocessedCatalogProperties,
            SessionPropertyManager sessionPropertyManager,
            Map<String, String> preparedStatements,
            ProtocolHeaders protocolHeaders)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.clientTransactionSupport = clientTransactionSupport;
        this.identity = requireNonNull(identity, "identity is null");
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
        this.clientTags = ImmutableSet.copyOf(requireNonNull(clientTags, "clientTags is null"));
        this.clientCapabilities = ImmutableSet.copyOf(requireNonNull(clientCapabilities, "clientCapabilities is null"));
        this.resourceEstimates = requireNonNull(resourceEstimates, "resourceEstimates is null");
        this.start = start;
        this.systemProperties = ImmutableMap.copyOf(requireNonNull(systemProperties, "systemProperties is null"));
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.preparedStatements = requireNonNull(preparedStatements, "preparedStatements is null");
        this.protocolHeaders = requireNonNull(protocolHeaders, "protocolHeaders is null");

        ImmutableMap.Builder<CatalogName, Map<String, String>> catalogPropertiesBuilder = ImmutableMap.builder();
        connectorProperties.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), ImmutableMap.copyOf(entry.getValue())))
                .forEach(catalogPropertiesBuilder::put);
        this.connectorProperties = catalogPropertiesBuilder.build();

        ImmutableMap.Builder<String, Map<String, String>> unprocessedCatalogPropertiesBuilder = ImmutableMap.builder();
        unprocessedCatalogProperties.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), ImmutableMap.copyOf(entry.getValue())))
                .forEach(unprocessedCatalogPropertiesBuilder::put);
        this.unprocessedCatalogProperties = unprocessedCatalogPropertiesBuilder.build();

        checkArgument(transactionId.isEmpty() || unprocessedCatalogProperties.isEmpty(), "Catalog session properties cannot be set if there is an open transaction");

        checkArgument(catalog.isPresent() || schema.isEmpty(), "schema is set but catalog is not");
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public String getUser()
    {
        return identity.getUser();
    }

    public Identity getIdentity()
    {
        return identity;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    public Optional<String> getSchema()
    {
        return schema;
    }

    public SqlPath getPath()
    {
        return path;
    }

    public TimeZoneKey getTimeZoneKey()
    {
        return timeZoneKey;
    }

    public Locale getLocale()
    {
        return locale;
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

    public Optional<String> getTraceToken()
    {
        return traceToken;
    }

    public ResourceEstimates getResourceEstimates()
    {
        return resourceEstimates;
    }

    public Instant getStart()
    {
        return start;
    }

    public Optional<TransactionId> getTransactionId()
    {
        return transactionId;
    }

    public TransactionId getRequiredTransactionId()
            throws NotInTransactionException
    {
        return transactionId.orElseThrow(NotInTransactionException::new);
    }

    public boolean isClientTransactionSupport()
    {
        return clientTransactionSupport;
    }

    public <T> T getSystemProperty(String name, Class<T> type)
    {
        return sessionPropertyManager.decodeSystemPropertyValue(name, systemProperties.get(name), type);
    }

    public Map<CatalogName, Map<String, String>> getConnectorProperties()
    {
        return connectorProperties;
    }

    public Map<String, String> getConnectorProperties(CatalogName catalogName)
    {
        return connectorProperties.getOrDefault(catalogName, ImmutableMap.of());
    }

    public Map<String, Map<String, String>> getUnprocessedCatalogProperties()
    {
        return unprocessedCatalogProperties;
    }

    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    public String getPreparedStatementFromExecute(Execute execute)
    {
        return getPreparedStatement(execute.getName().getValue());
    }

    public String getPreparedStatement(String name)
    {
        String sql = preparedStatements.get(name);
        checkCondition(sql != null, NOT_FOUND, "Prepared statement not found: " + name);
        return sql;
    }

    public ProtocolHeaders getProtocolHeaders()
    {
        return protocolHeaders;
    }

    public Session beginTransactionId(TransactionId transactionId, TransactionManager transactionManager, AccessControl accessControl)
    {
        requireNonNull(transactionId, "transactionId is null");
        checkArgument(this.transactionId.isEmpty(), "Session already has an active transaction");
        requireNonNull(transactionManager, "transactionManager is null");
        requireNonNull(accessControl, "accessControl is null");

        for (Entry<String, String> property : systemProperties.entrySet()) {
            // verify permissions
            accessControl.checkCanSetSystemSessionProperty(identity, property.getKey());

            // validate session property value
            sessionPropertyManager.validateSystemSessionProperty(property.getKey(), property.getValue());
        }

        // Now that there is a transaction, the catalog name can be resolved to a connector, and the catalog properties can be validated
        ImmutableMap.Builder<CatalogName, Map<String, String>> connectorProperties = ImmutableMap.builder();
        for (Entry<String, Map<String, String>> catalogEntry : unprocessedCatalogProperties.entrySet()) {
            String catalogName = catalogEntry.getKey();
            Map<String, String> catalogProperties = catalogEntry.getValue();
            if (catalogProperties.isEmpty()) {
                continue;
            }
            CatalogName catalog = transactionManager.getOptionalCatalogMetadata(transactionId, catalogName)
                    .orElseThrow(() -> new TrinoException(NOT_FOUND, "Session property catalog does not exist: " + catalogName))
                    .getCatalogName();

            for (Entry<String, String> property : catalogProperties.entrySet()) {
                // verify permissions
                accessControl.checkCanSetCatalogSessionProperty(new SecurityContext(transactionId, identity, queryId), catalogName, property.getKey());

                // validate session property value
                sessionPropertyManager.validateCatalogSessionProperty(catalog, catalogName, property.getKey(), property.getValue());
            }
            connectorProperties.put(catalog, catalogProperties);
        }

        ImmutableMap.Builder<String, SelectedRole> connectorRoles = ImmutableMap.builder();
        for (Entry<String, SelectedRole> entry : identity.getCatalogRoles().entrySet()) {
            String catalogName = entry.getKey();
            SelectedRole role = entry.getValue();
            CatalogName catalog = transactionManager.getOptionalCatalogMetadata(transactionId, catalogName)
                    .orElseThrow(() -> new TrinoException(NOT_FOUND, "Catalog for role does not exist: " + catalogName))
                    .getCatalogName();
            if (role.getType() == SelectedRole.Type.ROLE) {
                accessControl.checkCanSetCatalogRole(new SecurityContext(transactionId, identity, queryId), role.getRole().orElseThrow(), catalogName);
            }
            connectorRoles.put(catalog.getCatalogName(), role);

            String informationSchemaCatalogName = createInformationSchemaCatalogName(catalog).getCatalogName();
            if (transactionManager.getCatalogNames(transactionId).containsKey(informationSchemaCatalogName)) {
                connectorRoles.put(informationSchemaCatalogName, role);
            }

            String systemTablesCatalogName = createSystemTablesCatalogName(catalog).getCatalogName();
            if (transactionManager.getCatalogNames(transactionId).containsKey(systemTablesCatalogName)) {
                connectorRoles.put(systemTablesCatalogName, role);
            }
        }

        return new Session(
                queryId,
                Optional.of(transactionId),
                clientTransactionSupport,
                Identity.from(identity)
                        .withConnectorRoles(connectorRoles.build())
                        .build(),
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
                connectorProperties.build(),
                ImmutableMap.of(),
                sessionPropertyManager,
                preparedStatements,
                protocolHeaders);
    }

    public Session withDefaultProperties(Map<String, String> systemPropertyDefaults, Map<String, Map<String, String>> catalogPropertyDefaults)
    {
        requireNonNull(systemPropertyDefaults, "systemPropertyDefaults is null");
        requireNonNull(catalogPropertyDefaults, "catalogPropertyDefaults is null");

        // to remove this check properties must be authenticated and validated as in beginTransactionId
        checkState(
                this.transactionId.isEmpty() && this.connectorProperties.isEmpty(),
                "Session properties cannot be overridden once a transaction is active");

        Map<String, String> systemProperties = new HashMap<>();
        systemProperties.putAll(systemPropertyDefaults);
        systemProperties.putAll(this.systemProperties);

        Map<String, Map<String, String>> connectorProperties = catalogPropertyDefaults.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), new HashMap<>(entry.getValue())))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        for (Entry<String, Map<String, String>> catalogProperties : this.unprocessedCatalogProperties.entrySet()) {
            String catalog = catalogProperties.getKey();
            for (Entry<String, String> entry : catalogProperties.getValue().entrySet()) {
                connectorProperties.computeIfAbsent(catalog, id -> new HashMap<>())
                        .put(entry.getKey(), entry.getValue());
            }
        }

        return new Session(
                queryId,
                transactionId,
                clientTransactionSupport,
                identity,
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
                ImmutableMap.of(),
                connectorProperties,
                sessionPropertyManager,
                preparedStatements,
                protocolHeaders);
    }

    public ConnectorSession toConnectorSession()
    {
        return new FullConnectorSession(this, identity.toConnectorIdentity());
    }

    public ConnectorSession toConnectorSession(String catalogName)
    {
        return toConnectorSession(new CatalogName(catalogName));
    }

    public ConnectorSession toConnectorSession(CatalogName catalogName)
    {
        requireNonNull(catalogName, "catalogName is null");

        return new FullConnectorSession(
                this,
                identity.toConnectorIdentity(catalogName.getCatalogName()),
                connectorProperties.getOrDefault(catalogName, ImmutableMap.of()),
                catalogName,
                catalogName.getCatalogName(),
                sessionPropertyManager);
    }

    public SessionRepresentation toSessionRepresentation()
    {
        return new SessionRepresentation(
                queryId.toString(),
                transactionId,
                clientTransactionSupport,
                identity.getUser(),
                identity.getGroups(),
                identity.getPrincipal().map(Principal::toString),
                identity.getEnabledRoles(),
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
                connectorProperties,
                unprocessedCatalogProperties,
                identity.getCatalogRoles(),
                preparedStatements,
                protocolHeaders.getProtocolName());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("transactionId", transactionId)
                .add("user", getUser())
                .add("principal", getIdentity().getPrincipal().orElse(null))
                .add("source", source.orElse(null))
                .add("catalog", catalog.orElse(null))
                .add("schema", schema.orElse(null))
                .add("path", path)
                .add("traceToken", traceToken.orElse(null))
                .add("timeZoneKey", timeZoneKey)
                .add("locale", locale)
                .add("remoteUserAddress", remoteUserAddress.orElse(null))
                .add("userAgent", userAgent.orElse(null))
                .add("clientInfo", clientInfo.orElse(null))
                .add("clientTags", clientTags)
                .add("clientCapabilities", clientCapabilities)
                .add("resourceEstimates", resourceEstimates)
                .add("start", start)
                .omitNullValues()
                .toString();
    }

    public static SessionBuilder builder(SessionPropertyManager sessionPropertyManager)
    {
        return new SessionBuilder(sessionPropertyManager);
    }

    @VisibleForTesting
    public static SessionBuilder builder(Session session)
    {
        return new SessionBuilder(session);
    }

    public SecurityContext toSecurityContext()
    {
        return new SecurityContext(getRequiredTransactionId(), getIdentity(), queryId);
    }

    public static class SessionBuilder
    {
        private QueryId queryId;
        private TransactionId transactionId;
        private boolean clientTransactionSupport;
        private Identity identity;
        private String source;
        private String catalog;
        private String schema;
        private SqlPath path;
        private Optional<String> traceToken = Optional.empty();
        private TimeZoneKey timeZoneKey;
        private Locale locale;
        private String remoteUserAddress;
        private String userAgent;
        private String clientInfo;
        private Set<String> clientTags = ImmutableSet.of();
        private Set<String> clientCapabilities = ImmutableSet.of();
        private ResourceEstimates resourceEstimates;
        private Instant start = Instant.now();
        private final Map<String, String> systemProperties = new HashMap<>();
        private final Map<String, Map<String, String>> catalogSessionProperties = new HashMap<>();
        private final SessionPropertyManager sessionPropertyManager;
        private final Map<String, String> preparedStatements = new HashMap<>();
        private ProtocolHeaders protocolHeaders = TRINO_HEADERS;

        private SessionBuilder(SessionPropertyManager sessionPropertyManager)
        {
            this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        }

        private SessionBuilder(Session session)
        {
            requireNonNull(session, "session is null");
            checkArgument(session.getTransactionId().isEmpty(), "Session builder cannot be created from a session in a transaction");
            this.sessionPropertyManager = session.sessionPropertyManager;
            this.queryId = session.queryId;
            this.transactionId = session.transactionId.orElse(null);
            this.clientTransactionSupport = session.clientTransactionSupport;
            this.identity = session.identity;
            this.source = session.source.orElse(null);
            this.catalog = session.catalog.orElse(null);
            this.path = session.path;
            this.schema = session.schema.orElse(null);
            this.traceToken = requireNonNull(session.traceToken, "traceToken is null");
            this.timeZoneKey = session.timeZoneKey;
            this.locale = session.locale;
            this.remoteUserAddress = session.remoteUserAddress.orElse(null);
            this.userAgent = session.userAgent.orElse(null);
            this.clientInfo = session.clientInfo.orElse(null);
            this.clientTags = ImmutableSet.copyOf(session.clientTags);
            this.start = session.start;
            this.systemProperties.putAll(session.systemProperties);
            session.unprocessedCatalogProperties
                    .forEach((catalog, properties) -> catalogSessionProperties.put(catalog, new HashMap<>(properties)));
            this.preparedStatements.putAll(session.preparedStatements);
            this.protocolHeaders = session.protocolHeaders;
        }

        public SessionBuilder setQueryId(QueryId queryId)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            return this;
        }

        public SessionBuilder setTransactionId(TransactionId transactionId)
        {
            checkArgument(catalogSessionProperties.isEmpty(), "Catalog session properties cannot be set if there is an open transaction");
            this.transactionId = transactionId;
            return this;
        }

        public SessionBuilder setClientTransactionSupport()
        {
            this.clientTransactionSupport = true;
            return this;
        }

        public SessionBuilder setCatalog(String catalog)
        {
            this.catalog = catalog;
            return this;
        }

        public SessionBuilder setCatalog(Optional<String> catalog)
        {
            this.catalog = catalog.orElse(null);
            return this;
        }

        public SessionBuilder setLocale(Locale locale)
        {
            this.locale = locale;
            return this;
        }

        public SessionBuilder setRemoteUserAddress(String remoteUserAddress)
        {
            this.remoteUserAddress = remoteUserAddress;
            return this;
        }

        public SessionBuilder setRemoteUserAddress(Optional<String> remoteUserAddress)
        {
            this.remoteUserAddress = remoteUserAddress.orElse(null);
            return this;
        }

        public SessionBuilder setSchema(String schema)
        {
            this.schema = schema;
            return this;
        }

        public SessionBuilder setSchema(Optional<String> schema)
        {
            this.schema = schema.orElse(null);
            return this;
        }

        public SessionBuilder setPath(SqlPath path)
        {
            this.path = path;
            return this;
        }

        public SessionBuilder setPath(Optional<SqlPath> path)
        {
            this.path = path.orElse(null);
            return this;
        }

        public SessionBuilder setSource(String source)
        {
            this.source = source;
            return this;
        }

        public SessionBuilder setSource(Optional<String> source)
        {
            this.source = source.orElse(null);
            return this;
        }

        public SessionBuilder setTraceToken(Optional<String> traceToken)
        {
            this.traceToken = requireNonNull(traceToken, "traceToken is null");
            return this;
        }

        public SessionBuilder setStart(Instant start)
        {
            this.start = start;
            return this;
        }

        public SessionBuilder setTimeZoneKey(TimeZoneKey timeZoneKey)
        {
            this.timeZoneKey = timeZoneKey;
            return this;
        }

        public SessionBuilder setTimeZoneKey(Optional<TimeZoneKey> timeZoneKey)
        {
            this.timeZoneKey = timeZoneKey.orElse(null);
            return this;
        }

        public SessionBuilder setIdentity(Identity identity)
        {
            this.identity = identity;
            return this;
        }

        public SessionBuilder setUserAgent(String userAgent)
        {
            this.userAgent = userAgent;
            return this;
        }

        public SessionBuilder setUserAgent(Optional<String> userAgent)
        {
            this.userAgent = userAgent.orElse(null);
            return this;
        }

        public SessionBuilder setClientInfo(String clientInfo)
        {
            this.clientInfo = clientInfo;
            return this;
        }

        public SessionBuilder setClientInfo(Optional<String> clientInfo)
        {
            this.clientInfo = clientInfo.orElse(null);
            return this;
        }

        public SessionBuilder setClientTags(Set<String> clientTags)
        {
            this.clientTags = ImmutableSet.copyOf(clientTags);
            return this;
        }

        public SessionBuilder setClientCapabilities(Set<String> clientCapabilities)
        {
            this.clientCapabilities = ImmutableSet.copyOf(clientCapabilities);
            return this;
        }

        public SessionBuilder setResourceEstimates(ResourceEstimates resourceEstimates)
        {
            this.resourceEstimates = resourceEstimates;
            return this;
        }

        /**
         * Sets a system property for the session.  The property name and value must
         * only contain characters from US-ASCII and must not be for '='.
         */
        public SessionBuilder setSystemProperty(String propertyName, String propertyValue)
        {
            systemProperties.put(propertyName, propertyValue);
            return this;
        }

        /**
         * Sets system properties, discarding any system properties previously set.
         */
        public SessionBuilder setSystemProperties(Map<String, String> systemProperties)
        {
            requireNonNull(systemProperties, "systemProperties is null");
            this.systemProperties.clear();
            this.systemProperties.putAll(systemProperties);
            return this;
        }

        /**
         * Sets a catalog property for the session.  The property name and value must
         * only contain characters from US-ASCII and must not be for '='.
         */
        public SessionBuilder setCatalogSessionProperty(String catalogName, String propertyName, String propertyValue)
        {
            checkArgument(transactionId == null, "Catalog session properties cannot be set if there is an open transaction");
            catalogSessionProperties.computeIfAbsent(catalogName, id -> new HashMap<>()).put(propertyName, propertyValue);
            return this;
        }

        public SessionBuilder addPreparedStatement(String statementName, String query)
        {
            this.preparedStatements.put(statementName, query);
            return this;
        }

        public SessionBuilder setProtocolHeaders(ProtocolHeaders protocolHeaders)
        {
            this.protocolHeaders = requireNonNull(protocolHeaders, "protocolHeaders is null");
            return this;
        }

        public Session build()
        {
            return new Session(
                    queryId,
                    Optional.ofNullable(transactionId),
                    clientTransactionSupport,
                    identity,
                    Optional.ofNullable(source),
                    Optional.ofNullable(catalog),
                    Optional.ofNullable(schema),
                    path != null ? path : new SqlPath(Optional.empty()),
                    traceToken,
                    timeZoneKey != null ? timeZoneKey : TimeZoneKey.getTimeZoneKey(TimeZone.getDefault().getID()),
                    locale != null ? locale : Locale.getDefault(),
                    Optional.ofNullable(remoteUserAddress),
                    Optional.ofNullable(userAgent),
                    Optional.ofNullable(clientInfo),
                    clientTags,
                    clientCapabilities,
                    Optional.ofNullable(resourceEstimates).orElse(new ResourceEstimateBuilder().build()),
                    start,
                    systemProperties,
                    ImmutableMap.of(),
                    catalogSessionProperties,
                    sessionPropertyManager,
                    preparedStatements,
                    protocolHeaders);
        }
    }

    public static class ResourceEstimateBuilder
    {
        private Optional<Duration> executionTime = Optional.empty();
        private Optional<Duration> cpuTime = Optional.empty();
        private Optional<DataSize> peakMemory = Optional.empty();

        public ResourceEstimateBuilder setExecutionTime(Duration executionTime)
        {
            this.executionTime = Optional.of(executionTime);
            return this;
        }

        public ResourceEstimateBuilder setCpuTime(Duration cpuTime)
        {
            this.cpuTime = Optional.of(cpuTime);
            return this;
        }

        public ResourceEstimateBuilder setPeakMemory(DataSize peakMemory)
        {
            this.peakMemory = Optional.of(peakMemory);
            return this;
        }

        public ResourceEstimates build()
        {
            return new ResourceEstimates(
                    executionTime.map(Duration::toMillis).map(java.time.Duration::ofMillis),
                    cpuTime.map(Duration::toMillis).map(java.time.Duration::ofMillis),
                    peakMemory.map(DataSize::toBytes));
        }
    }
}
