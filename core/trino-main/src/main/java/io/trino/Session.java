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
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.client.ProtocolHeaders;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogSchemaName;
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
import java.util.List;
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
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.sql.SqlPath.EMPTY_PATH;
import static io.trino.util.Failures.checkCondition;
import static java.util.Objects.requireNonNull;

public final class Session
{
    private final QueryId queryId;
    private final Span querySpan;
    private final Optional<TransactionId> transactionId;
    private final boolean clientTransactionSupport;
    private final Identity identity;
    private final Identity originalIdentity;
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
    private final Map<String, Map<String, String>> catalogProperties;
    private final SessionPropertyManager sessionPropertyManager;
    private final Map<String, String> preparedStatements;
    private final ProtocolHeaders protocolHeaders;
    private final Optional<Slice> exchangeEncryptionKey;
    private final Optional<String> queryDataEncoding;

    public Session(
            QueryId queryId,
            Span querySpan,
            Optional<TransactionId> transactionId,
            boolean clientTransactionSupport,
            Identity identity,
            Identity originalIdentity,
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
            Map<String, Map<String, String>> catalogProperties,
            SessionPropertyManager sessionPropertyManager,
            Map<String, String> preparedStatements,
            ProtocolHeaders protocolHeaders,
            Optional<Slice> exchangeEncryptionKey,
            Optional<String> queryDataEncoding)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.querySpan = requireNonNull(querySpan, "querySpan is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.clientTransactionSupport = clientTransactionSupport;
        this.identity = requireNonNull(identity, "identity is null");
        this.originalIdentity = requireNonNull(originalIdentity, "originalIdentity is null");
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
        this.exchangeEncryptionKey = requireNonNull(exchangeEncryptionKey, "exchangeEncryptionKey is null");
        this.queryDataEncoding = requireNonNull(queryDataEncoding, "queryDataEncoding is null");

        requireNonNull(catalogProperties, "catalogProperties is null");
        ImmutableMap.Builder<String, Map<String, String>> catalogPropertiesBuilder = ImmutableMap.builder();
        catalogProperties.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), ImmutableMap.copyOf(entry.getValue())))
                .forEach(catalogPropertiesBuilder::put);
        this.catalogProperties = catalogPropertiesBuilder.buildOrThrow();

        checkArgument(catalog.isPresent() || schema.isEmpty(), "schema is set but catalog is not");
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public Span getQuerySpan()
    {
        return querySpan;
    }

    public String getUser()
    {
        return identity.getUser();
    }

    public Identity getIdentity()
    {
        return identity;
    }

    public Identity getOriginalIdentity()
    {
        return originalIdentity;
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
        // Allow overriding timezone key with a session property regardless of it's source
        return SystemSessionProperties.getTimeZoneId(this)
                .map(TimeZoneKey::getTimeZoneKey)
                .orElse(timeZoneKey);
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

    public Map<String, Map<String, String>> getCatalogProperties()
    {
        return catalogProperties;
    }

    public Map<String, String> getCatalogProperties(String catalogName)
    {
        return catalogProperties.getOrDefault(catalogName, ImmutableMap.of());
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
        checkCondition(sql != null, NOT_FOUND, "Prepared statement not found: %s", name);
        return sql;
    }

    public ProtocolHeaders getProtocolHeaders()
    {
        return protocolHeaders;
    }

    public Optional<Slice> getExchangeEncryptionKey()
    {
        return exchangeEncryptionKey;
    }

    public Optional<String> getQueryDataEncoding()
    {
        return queryDataEncoding;
    }

    public SessionPropertyManager getSessionPropertyManager()
    {
        return sessionPropertyManager;
    }

    public Session beginTransactionId(TransactionId transactionId, TransactionManager transactionManager, AccessControl accessControl)
    {
        requireNonNull(transactionId, "transactionId is null");
        checkArgument(this.transactionId.isEmpty(), "Session already has an active transaction");
        requireNonNull(transactionManager, "transactionManager is null");
        requireNonNull(accessControl, "accessControl is null");

        validateSystemProperties(accessControl, this.systemProperties);

        // Now that there is a transaction, the catalog name can be resolved to a connector, and the catalog properties can be validated
        ImmutableMap.Builder<String, Map<String, String>> connectorProperties = ImmutableMap.builder();
        for (Entry<String, Map<String, String>> catalogEntry : this.catalogProperties.entrySet()) {
            String catalogName = catalogEntry.getKey();
            Map<String, String> catalogProperties = catalogEntry.getValue();
            if (catalogProperties.isEmpty()) {
                continue;
            }
            CatalogHandle catalogHandle = transactionManager.getCatalogHandle(transactionId, catalogName)
                    .orElseThrow(() -> new TrinoException(CATALOG_NOT_FOUND, "Catalog '%s' not found".formatted(catalogName)));

            validateCatalogProperties(Optional.of(transactionId), accessControl, catalogName, catalogHandle, catalogProperties);
            connectorProperties.put(catalogName, catalogProperties);
        }

        ImmutableMap.Builder<String, SelectedRole> connectorRoles = ImmutableMap.builder();
        for (Entry<String, SelectedRole> entry : identity.getCatalogRoles().entrySet()) {
            String catalogName = entry.getKey();
            SelectedRole role = entry.getValue();
            if (transactionManager.getCatalogHandle(transactionId, catalogName).isEmpty()) {
                throw new TrinoException(CATALOG_NOT_FOUND, "Catalog '%s' not found".formatted(catalogName));
            }
            if (role.getType() == SelectedRole.Type.ROLE) {
                accessControl.checkCanSetCatalogRole(new SecurityContext(transactionId, identity, queryId, start), role.getRole().orElseThrow(), catalogName);
            }
            connectorRoles.put(catalogName, role);
        }

        return new Session(
                queryId,
                querySpan,
                Optional.of(transactionId),
                clientTransactionSupport,
                Identity.from(identity)
                        .withConnectorRoles(connectorRoles.buildOrThrow())
                        .build(),
                originalIdentity,
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
                connectorProperties.buildOrThrow(),
                sessionPropertyManager,
                preparedStatements,
                protocolHeaders,
                exchangeEncryptionKey,
                queryDataEncoding);
    }

    public Session withDefaultProperties(Map<String, String> systemPropertyDefaults, Map<String, Map<String, String>> catalogPropertyDefaults, AccessControl accessControl)
    {
        requireNonNull(systemPropertyDefaults, "systemPropertyDefaults is null");
        requireNonNull(catalogPropertyDefaults, "catalogPropertyDefaults is null");

        checkState(transactionId.isEmpty(), "property defaults can not be added to a transaction already in progress");

        // NOTE: properties should not be validated here and instead will be validated in beginTransactionId
        Map<String, String> systemProperties = new HashMap<>();
        systemProperties.putAll(systemPropertyDefaults);
        systemProperties.putAll(this.systemProperties);

        Map<String, Map<String, String>> catalogProperties = catalogPropertyDefaults.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), new HashMap<>(entry.getValue())))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        for (Entry<String, Map<String, String>> catalogEntry : this.catalogProperties.entrySet()) {
            catalogProperties.computeIfAbsent(catalogEntry.getKey(), id -> new HashMap<>())
                    .putAll(catalogEntry.getValue());
        }

        return withProperties(systemProperties, catalogProperties);
    }

    public Session withProperties(Map<String, String> systemProperties, Map<String, Map<String, String>> catalogProperties)
    {
        return new Session(
                queryId,
                querySpan,
                transactionId,
                clientTransactionSupport,
                identity,
                originalIdentity,
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
                protocolHeaders,
                exchangeEncryptionKey,
                queryDataEncoding);
    }

    public Session withExchangeEncryption(Slice encryptionKey)
    {
        checkState(exchangeEncryptionKey.isEmpty(), "exchangeEncryptionKey is already present");
        return new Session(
                queryId,
                querySpan,
                transactionId,
                clientTransactionSupport,
                identity,
                originalIdentity,
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
                protocolHeaders,
                Optional.of(encryptionKey),
                queryDataEncoding);
    }

    public Session withoutSpooling()
    {
        return new Session(
                queryId,
                querySpan,
                transactionId,
                clientTransactionSupport,
                identity,
                originalIdentity,
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
                protocolHeaders,
                exchangeEncryptionKey,
                Optional.empty());
    }

    public ConnectorSession toConnectorSession()
    {
        return new FullConnectorSession(this, identity.toConnectorIdentity());
    }

    public ConnectorSession toConnectorSession(CatalogHandle catalogHandle)
    {
        requireNonNull(catalogHandle, "catalogHandle is null");

        String catalogName = catalogHandle.getCatalogName().toString();
        return new FullConnectorSession(
                this,
                identity.toConnectorIdentity(catalogName),
                catalogProperties.getOrDefault(catalogName, ImmutableMap.of()),
                catalogHandle,
                catalogName,
                sessionPropertyManager);
    }

    public SessionRepresentation toSessionRepresentation()
    {
        return new SessionRepresentation(
                queryId.toString(),
                querySpan,
                transactionId,
                clientTransactionSupport,
                identity.getUser(),
                originalIdentity.getUser(),
                identity.getGroups(),
                originalIdentity.getGroups(),
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
                catalogProperties,
                identity.getCatalogRoles(),
                preparedStatements,
                protocolHeaders.getProtocolName(),
                queryDataEncoding);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("querySpan", querySpanString().orElse(null))
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

    private Optional<String> querySpanString()
    {
        return Optional.of(querySpan)
                .filter(span -> span.getSpanContext().isValid())
                .map(span -> toStringHelper("Span")
                        .add("spanId", span.getSpanContext().getSpanId())
                        .add("traceId", span.getSpanContext().getTraceId())
                        .toString());
    }

    private void validateCatalogProperties(
            Optional<TransactionId> transactionId,
            AccessControl accessControl,
            String catalogName,
            CatalogHandle catalogHandle,
            Map<String, String> catalogProperties)
    {
        for (Entry<String, String> property : catalogProperties.entrySet()) {
            // verify permissions
            if (transactionId.isPresent()) {
                accessControl.checkCanSetCatalogSessionProperty(new SecurityContext(transactionId.get(), identity, queryId, start), catalogName, property.getKey());
            }

            // validate catalog session property value
            sessionPropertyManager.validateCatalogSessionProperty(catalogName, catalogHandle, property.getKey(), property.getValue());
        }
    }

    private void validateSystemProperties(AccessControl accessControl, Map<String, String> systemProperties)
    {
        for (Entry<String, String> property : systemProperties.entrySet()) {
            // verify permissions
            accessControl.checkCanSetSystemSessionProperty(identity, queryId, property.getKey());

            // validate session property value
            sessionPropertyManager.validateSystemSessionProperty(property.getKey(), property.getValue());
        }
    }

    public Session createViewSession(Optional<String> catalog, Optional<String> schema, Identity identity, List<CatalogSchemaName> viewPath)
    {
        return createViewSession(catalog, schema, identity, path.forView(viewPath));
    }

    public Session createViewSession(Optional<String> catalog, Optional<String> schema, Identity identity, SqlPath sqlPath)
    {
        return builder(sessionPropertyManager)
                .setQueryId(getQueryId())
                .setTransactionId(getTransactionId().orElse(null))
                .setIdentity(identity)
                .setOriginalIdentity(getOriginalIdentity())
                .setSource(getSource().orElse(null))
                .setCatalog(catalog)
                .setSchema(schema)
                .setPath(sqlPath)
                .setTimeZoneKey(getTimeZoneKey())
                .setLocale(getLocale())
                .setRemoteUserAddress(getRemoteUserAddress().orElse(null))
                .setUserAgent(getUserAgent().orElse(null))
                .setClientInfo(getClientInfo().orElse(null))
                .setStart(getStart())
                .build();
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
        return new SecurityContext(getRequiredTransactionId(), getIdentity(), queryId, start);
    }

    public static class SessionBuilder
    {
        private QueryId queryId;
        private Span querySpan = Span.getInvalid();
        private TransactionId transactionId;
        private boolean clientTransactionSupport;
        private Identity identity;
        private Identity originalIdentity;
        private String source;
        private String catalog;
        private String schema;
        private SqlPath path = EMPTY_PATH;
        private Optional<String> traceToken = Optional.empty();
        private TimeZoneKey timeZoneKey;
        private Locale locale;
        private String remoteUserAddress;
        private String userAgent;
        private String clientInfo;
        private Set<String> clientTags = ImmutableSet.of();
        private Set<String> clientCapabilities = ImmutableSet.of();
        private Optional<String> queryDataEncoding = Optional.empty();
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
            this.originalIdentity = session.originalIdentity;
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
            this.clientCapabilities = ImmutableSet.copyOf(session.clientCapabilities);
            this.queryDataEncoding = session.queryDataEncoding;
            this.clientTags = ImmutableSet.copyOf(session.clientTags);
            this.start = session.start;
            this.systemProperties.putAll(session.systemProperties);
            session.catalogProperties
                    .forEach((catalog, properties) -> catalogSessionProperties.put(catalog, new HashMap<>(properties)));
            this.preparedStatements.putAll(session.preparedStatements);
            this.protocolHeaders = session.protocolHeaders;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setQueryId(QueryId queryId)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setQuerySpan(Span querySpan)
        {
            this.querySpan = requireNonNull(querySpan, "querySpan is null");
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setTransactionId(TransactionId transactionId)
        {
            checkArgument(catalogSessionProperties.isEmpty(), "Catalog session properties cannot be set if there is an open transaction");
            this.transactionId = transactionId;
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setClientTransactionSupport()
        {
            this.clientTransactionSupport = true;
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setCatalog(String catalog)
        {
            this.catalog = catalog;
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setCatalog(Optional<String> catalog)
        {
            this.catalog = catalog.orElse(null);
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setLocale(Locale locale)
        {
            this.locale = locale;
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setRemoteUserAddress(String remoteUserAddress)
        {
            this.remoteUserAddress = remoteUserAddress;
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setRemoteUserAddress(Optional<String> remoteUserAddress)
        {
            this.remoteUserAddress = remoteUserAddress.orElse(null);
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setSchema(String schema)
        {
            this.schema = schema;
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setSchema(Optional<String> schema)
        {
            this.schema = schema.orElse(null);
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setPath(SqlPath path)
        {
            this.path = path;
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setSource(String source)
        {
            this.source = source;
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setSource(Optional<String> source)
        {
            this.source = source.orElse(null);
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setTraceToken(Optional<String> traceToken)
        {
            this.traceToken = requireNonNull(traceToken, "traceToken is null");
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setStart(Instant start)
        {
            this.start = start;
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setTimeZoneKey(TimeZoneKey timeZoneKey)
        {
            this.timeZoneKey = timeZoneKey;
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setTimeZoneKey(Optional<TimeZoneKey> timeZoneKey)
        {
            this.timeZoneKey = timeZoneKey.orElse(null);
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setIdentity(Identity identity)
        {
            this.identity = identity;
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setOriginalIdentity(Identity originalIdentity)
        {
            this.originalIdentity = originalIdentity;
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setUserAgent(String userAgent)
        {
            this.userAgent = userAgent;
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setUserAgent(Optional<String> userAgent)
        {
            this.userAgent = userAgent.orElse(null);
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setClientInfo(String clientInfo)
        {
            this.clientInfo = clientInfo;
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setClientInfo(Optional<String> clientInfo)
        {
            this.clientInfo = clientInfo.orElse(null);
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setClientTags(Set<String> clientTags)
        {
            this.clientTags = ImmutableSet.copyOf(clientTags);
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setClientCapabilities(Set<String> clientCapabilities)
        {
            this.clientCapabilities = ImmutableSet.copyOf(clientCapabilities);
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setResourceEstimates(ResourceEstimates resourceEstimates)
        {
            this.resourceEstimates = resourceEstimates;
            return this;
        }

        /**
         * Sets a system property for the session.  The property name and value must
         * only contain characters from US-ASCII and must not be for '='.
         */
        @CanIgnoreReturnValue
        public SessionBuilder setSystemProperty(String propertyName, String propertyValue)
        {
            systemProperties.put(propertyName, propertyValue);
            return this;
        }

        /**
         * Sets system properties, discarding any system properties previously set.
         */
        @CanIgnoreReturnValue
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
        @CanIgnoreReturnValue
        public SessionBuilder setCatalogSessionProperty(String catalogName, String propertyName, String propertyValue)
        {
            checkArgument(transactionId == null, "Catalog session properties cannot be set if there is an open transaction");
            catalogSessionProperties.computeIfAbsent(catalogName, id -> new HashMap<>()).put(propertyName, propertyValue);
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder addPreparedStatement(String statementName, String query)
        {
            this.preparedStatements.put(statementName, query);
            return this;
        }

        @CanIgnoreReturnValue
        public SessionBuilder setProtocolHeaders(ProtocolHeaders protocolHeaders)
        {
            this.protocolHeaders = requireNonNull(protocolHeaders, "protocolHeaders is null");
            return this;
        }

        public SessionBuilder setQueryDataEncoding(Optional<String> value)
        {
            this.queryDataEncoding = value;
            return this;
        }

        public Session build()
        {
            return new Session(
                    queryId,
                    querySpan,
                    Optional.ofNullable(transactionId),
                    clientTransactionSupport,
                    identity,
                    originalIdentity,
                    Optional.ofNullable(source),
                    Optional.ofNullable(catalog),
                    Optional.ofNullable(schema),
                    path,
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
                    catalogSessionProperties,
                    sessionPropertyManager,
                    preparedStatements,
                    protocolHeaders,
                    Optional.empty(),
                    queryDataEncoding);
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
