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
package io.prestosql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.spi.Name;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.security.BasicPrincipal;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.spi.session.ResourceEstimates;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.sql.SqlPath;
import io.prestosql.transaction.TransactionId;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public final class SessionRepresentation
{
    private final String queryId;
    private final Optional<TransactionId> transactionId;
    private final boolean clientTransactionSupport;
    private final Name user;
    private final Optional<Name> principal;
    private final Optional<String> source;
    private final Optional<Name> catalog;
    private final Optional<Name> schema;
    private final SqlPath path;
    private final Optional<String> traceToken;
    private final TimeZoneKey timeZoneKey;
    private final Locale locale;
    private final Optional<String> remoteUserAddress;
    private final Optional<String> userAgent;
    private final Optional<String> clientInfo;
    private final Set<String> clientTags;
    private final Set<String> clientCapabilities;
    private final long startTime;
    private final ResourceEstimates resourceEstimates;
    private final Map<String, String> systemProperties;
    private final Map<CatalogName, Map<String, String>> catalogProperties;
    private final Map<String, Map<String, String>> unprocessedCatalogProperties;
    private final List<SelectedRoleInformation> roleInformation;
    private final List<CatalogPropertiesInformation> propertiesInformation;
    private final Map<Name, SelectedRole> roles;
    private final Map<String, String> preparedStatements;

    @JsonCreator
    public SessionRepresentation(
            @JsonProperty("queryId") String queryId,
            @JsonProperty("transactionId") Optional<TransactionId> transactionId,
            @JsonProperty("clientTransactionSupport") boolean clientTransactionSupport,
            @JsonProperty("user") Name user,
            @JsonProperty("principal") Optional<Name> principal,
            @JsonProperty("source") Optional<String> source,
            @JsonProperty("catalog") Optional<Name> catalog,
            @JsonProperty("schema") Optional<Name> schema,
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
            @JsonProperty("startTime") long startTime,
            @JsonProperty("systemProperties") Map<String, String> systemProperties,
            @JsonProperty("propertiesInformation") List<CatalogPropertiesInformation> propertiesInformation,
            @JsonProperty("unprocessedCatalogProperties") Map<String, Map<String, String>> unprocessedCatalogProperties,
            @JsonProperty("roleInformation") List<SelectedRoleInformation> roleInformation,
            @JsonProperty("preparedStatements") Map<String, String> preparedStatements)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.clientTransactionSupport = clientTransactionSupport;
        this.user = requireNonNull(user, "user is null");
        this.principal = requireNonNull(principal, "principal is null");
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
        this.startTime = startTime;
        this.systemProperties = ImmutableMap.copyOf(systemProperties);
        this.roleInformation = ImmutableList.copyOf(requireNonNull(roleInformation, "RoleInformation is null"));
        this.propertiesInformation = ImmutableList.copyOf(requireNonNull(propertiesInformation, "PropertiesInformation is null"));
        this.roles = ImmutableMap.copyOf(this.roleInformation.stream().collect(Collectors.toMap(x -> x.getName(), x -> x.getSelectedRole())));
        this.preparedStatements = ImmutableMap.copyOf(preparedStatements);

        ImmutableMap.Builder<CatalogName, Map<String, String>> catalogPropertiesBuilder = ImmutableMap.builder();
        for (CatalogPropertiesInformation information : this.propertiesInformation) {
            catalogPropertiesBuilder.put(information.getCatalogName(), information.getProperties());
        }
        this.catalogProperties = catalogPropertiesBuilder.build();

        ImmutableMap.Builder<String, Map<String, String>> unprocessedCatalogPropertiesBuilder = ImmutableMap.builder();
        for (Entry<String, Map<String, String>> entry : unprocessedCatalogProperties.entrySet()) {
            unprocessedCatalogPropertiesBuilder.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
        }
        this.unprocessedCatalogProperties = unprocessedCatalogPropertiesBuilder.build();
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
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
    public Name getUser()
    {
        return user;
    }

    @JsonProperty
    public Optional<Name> getPrincipal()
    {
        return principal;
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
    public Optional<Name> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public Optional<Name> getSchema()
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
    public long getStartTime()
    {
        return startTime;
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

    public Map<CatalogName, Map<String, String>> getCatalogProperties()
    {
        return catalogProperties;
    }

    @JsonProperty
    public Map<String, Map<String, String>> getUnprocessedCatalogProperties()
    {
        return unprocessedCatalogProperties;
    }

    @JsonProperty
    public List<SelectedRoleInformation> getRoleInformation()
    {
        return roleInformation;
    }

    @JsonProperty
    public List<CatalogPropertiesInformation> getPropertiesInformation()
    {
        return propertiesInformation;
    }

    @JsonProperty
    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }

    public Session toSession(SessionPropertyManager sessionPropertyManager)
    {
        return toSession(sessionPropertyManager, emptyMap());
    }

    public Session toSession(SessionPropertyManager sessionPropertyManager, Map<String, String> extraCredentials)
    {
        return new Session(
                new QueryId(queryId),
                transactionId,
                clientTransactionSupport,
                new Identity(user, principal.map(BasicPrincipal::new), roles, extraCredentials),
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
                startTime,
                systemProperties,
                catalogProperties,
                unprocessedCatalogProperties,
                sessionPropertyManager,
                preparedStatements);
    }

    //For JsonCreator
    public static class SelectedRoleInformation
    {
        private final Name name;
        private final SelectedRole selectedRole;

        @JsonCreator
        public SelectedRoleInformation(@JsonProperty("name") Name name, @JsonProperty("selectedRole") SelectedRole selectedRole)
        {
            this.name = requireNonNull(name, "Name is null");
            this.selectedRole = requireNonNull(selectedRole, "SelectedROle is null");
        }

        @JsonProperty("name")
        public Name getName()
        {
            return this.name;
        }

        @JsonProperty("selectedRole")
        public SelectedRole getSelectedRole()
        {
            return this.selectedRole;
        }
    }

    //For JsonCreator
    public static class CatalogPropertiesInformation
    {
        private final CatalogName catalogName;
        private final Map<String, String> properties;

        @JsonCreator
        public CatalogPropertiesInformation(@JsonProperty("catalogName") CatalogName catalogName, @JsonProperty("properties") Map<String, String> properties)
        {
            this.catalogName = requireNonNull(catalogName, "CatalogName is null");
            this.properties = ImmutableMap.copyOf(requireNonNull(properties, "SelectedROle is null"));
        }

        @JsonProperty("catalogName")
        public CatalogName getCatalogName()
        {
            return this.catalogName;
        }

        @JsonProperty("properties")
        public Map<String, String> getProperties()
        {
            return this.properties;
        }
    }
}
