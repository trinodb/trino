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
package io.trino.plugin.hive.metastore.polaris;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StaticBodyGenerator;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * REST client for Apache Polaris catalog API.
 *
 * This client follows the TrinoRestCatalog pattern:
 * - Delegates standard Iceberg operations to RESTSessionCatalog
 * - Uses direct HttpClient for Polaris-specific Generic Table operations
 * - Provides unified interface for both Iceberg and Delta Lake table operations
 */
public class PolarisRestClient
{
    private final RESTSessionCatalog restSessionCatalog;
    private final HttpClient httpClient;
    private final PolarisMetastoreConfig config;
    private final SecurityProperties securityProperties;
    private final ObjectMapper objectMapper;

    @Inject
    public PolarisRestClient(
            RESTSessionCatalog restSessionCatalog,
            @ForPolarisClient HttpClient httpClient,
            PolarisMetastoreConfig config,
            SecurityProperties securityProperties,
            ObjectMapper objectMapper)
    {
        this.restSessionCatalog = requireNonNull(restSessionCatalog, "restSessionCatalog is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.config = requireNonNull(config, "config is null");
        this.securityProperties = requireNonNull(securityProperties, "securityProperties is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    // ICEBERG OPERATIONS (via RESTSessionCatalog)

    /**
     * Lists Iceberg tables using the standard REST catalog
     */
    public List<PolarisTableIdentifier> listIcebergTables(String namespaceName)
    {
        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            Namespace namespace = Namespace.of(namespaceName.split("\\."));

            return restSessionCatalog.listTables(sessionContext, namespace).stream()
                    .map(id -> new PolarisTableIdentifier(id.namespace().toString(), id.name()))
                    .collect(toImmutableList());
        }
        catch (RESTException e) {
            throw new PolarisException("Failed to list Iceberg tables: " + e.getMessage(), e);
        }
    }

    /**
     * Loads Iceberg table metadata using the standard REST catalog
     */
    public PolarisTableMetadata loadIcebergTable(String namespaceName, String tableName)
    {
        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            TableIdentifier tableId = TableIdentifier.of(namespaceName, tableName);

            org.apache.iceberg.Table table = restSessionCatalog.loadTable(sessionContext, tableId);
            return convertIcebergTableToPolaris(table);
        }
        catch (NoSuchTableException e) {
            throw new PolarisNotFoundException("Iceberg table not found: " + namespaceName + "." + tableName);
        }
        catch (RESTException e) {
            throw new PolarisException("Failed to load Iceberg table: " + e.getMessage(), e);
        }
    }

    // NAMESPACE OPERATIONS (via RESTSessionCatalog)

    /**
     * Lists namespaces using the standard REST catalog
     */
    public List<PolarisNamespace> listNamespaces(Optional<String> parent)
    {
        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            Namespace parentNamespace = parent.map(p -> Namespace.of(p.split("\\."))).orElse(Namespace.empty());

            return restSessionCatalog.listNamespaces(sessionContext, parentNamespace).stream()
                    .map(ns -> new PolarisNamespace(ns.toString(), ImmutableMap.of()))
                    .collect(toImmutableList());
        }
        catch (RESTException e) {
            throw new PolarisException("Failed to list namespaces: " + e.getMessage(), e);
        }
    }

    /**
     * Creates namespace using the standard REST catalog
     */
    public void createNamespace(PolarisNamespace namespace)
    {
        // TODO: Implement REST API call
    }

    // GENERIC TABLE OPERATIONS (via HttpClient)

    /**
     * Lists Generic (Delta Lake, CSV, etc.) tables using Polaris-specific API
     */
    public List<PolarisTableIdentifier> listGenericTables(String namespaceName)
    {
        URI uri = buildUri("/polaris/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/generic-tables");

        Request request = prepareGet()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        return execute(request, new ResponseHandler<List<PolarisTableIdentifier>, RuntimeException>()
        {
            @Override
            public List<PolarisTableIdentifier> handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to list generic tables", exception);
            }

            @Override
            public List<PolarisTableIdentifier> handle(Request request, Response response)
            {
                if (response.getStatusCode() != 200) {
                    throw new PolarisException("Failed to list generic tables: " + response.getStatusCode());
                }
                try {
                    ListGenericTablesResponse listResponse = objectMapper.readValue(response.getInputStream(), ListGenericTablesResponse.class);
                    return listResponse.toPolarisTableIdentifiers();
                }
                catch (IOException e) {
                    throw new PolarisException("Failed to parse generic tables response", e);
                }
            }
        });
    }

    /**
     * Loads a Generic table using Polaris-specific API
     */
    public PolarisGenericTable loadGenericTable(String namespaceName, String tableName)
    {
        URI uri = buildUri("/polaris/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(namespaceName) + "/generic-tables/" + tableName);

        Request request = prepareGet()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        return execute(request, new ResponseHandler<PolarisGenericTable, RuntimeException>()
        {
            @Override
            public PolarisGenericTable handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to load generic table: " + tableName, exception);
            }

            @Override
            public PolarisGenericTable handle(Request request, Response response)
            {
                if (response.getStatusCode() == 404) {
                    throw new TableNotFoundException(new SchemaTableName(namespaceName, tableName));
                }
                if (response.getStatusCode() != 200) {
                    throw new PolarisException("Failed to load generic table: " + response.getStatusCode());
                }
                try {
                    LoadGenericTableResponse loadResponse = objectMapper.readValue(response.getInputStream(), LoadGenericTableResponse.class);
                    return loadResponse.getTable();
                }
                catch (IOException e) {
                    throw new PolarisException("Failed to parse load generic table response", e);
                }
            }
        });
    }

    /**
     * Creates a Generic table using Polaris-specific API
     */
    public void createGenericTable(String databaseName, PolarisGenericTable genericTable)
    {
        // TODO: Implement REST API call
    }

    /**
     * Drops a Generic table using Polaris-specific API
     */
    public void dropGenericTable(String databaseName, String tableName)
    {
        // TODO: Implement REST API call
    }

    // HELPER METHODS

    /**
     * Creates session context for Iceberg operations
     */
    private SessionCatalog.SessionContext createSessionContext()
    {
        String sessionId = UUID.randomUUID().toString();
        Map<String, String> credentials = getAuthHeaders();
        Map<String, String> properties = ImmutableMap.of(
                "catalog", config.getPrefix(),
                "warehouse", config.getUri().toString());

        return new SessionCatalog.SessionContext(sessionId, "polaris-user", credentials, properties, null);
    }

    /**
     * Converts Iceberg Table to PolarisTableMetadata
     */
    private PolarisTableMetadata convertIcebergTableToPolaris(org.apache.iceberg.Table table)
    {
        // Extract metadata from Iceberg table
        TableOperations ops = ((HasTableOperations) table).operations();
        String location = ops.current().location();
        Map<String, String> properties = ops.current().properties();

        // Convert Iceberg schema to map representation
        Schema icebergSchema = ops.current().schema();
        Map<String, Object> schemaMap = ImmutableMap.of(
                "type", "struct",
                "schema-id", icebergSchema.schemaId(),
                "fields", icebergSchema.columns().stream()
                        .map(field -> ImmutableMap.of(
                                "id", field.fieldId(),
                                "name", field.name(),
                                "required", field.isRequired(),
                                "type", field.type().toString()))
                        .collect(toImmutableList()));

        return new PolarisTableMetadata(
                    location,
                    schemaMap,
                    properties);
    }

    // AUTHENTICATION & HTTP UTILITIES

    /**
     * Gets authentication headers using the same SecurityProperties as RESTSessionCatalog
     */
    private Map<String, String> getAuthHeaders()
    {
        ImmutableMap.Builder<String, String> headers = ImmutableMap.builder();
        Map<String, String> securityProps = securityProperties.get();

        // Extract token or credential from security properties
        if (securityProps.containsKey(OAuth2Properties.TOKEN)) {
            headers.put("Authorization", "Bearer " + securityProps.get(OAuth2Properties.TOKEN));
        }
        else if (securityProps.containsKey(OAuth2Properties.CREDENTIAL)) {
            // For credential-based auth, we would need to get the actual token from OAuth2 flow
            // For now, we'll need to implement token extraction from RESTSessionCatalog
            // This is a placeholder - in practice, we'd extract the active token
            headers.put("Authorization", "Bearer " + "PLACEHOLDER_TOKEN");
        }

        return headers.buildOrThrow();
    }

    /**
     * Builds headers for HTTP requests
     */
    private Multimap<String, String> buildHeaders(Map<String, String> headers)
    {
        ImmutableMultimap.Builder<String, String> builder = ImmutableMultimap.builder();
        headers.forEach(builder::put);
        return builder.build();
    }

    /**
     * Executes HTTP request with error handling
     */
    private <T> T execute(Request request, ResponseHandler<T, RuntimeException> responseHandler)
    {
        try {
            return httpClient.execute(request, responseHandler);
        }
        catch (Exception e) {
            throw new PolarisException("Request failed: " + e.getMessage(), e);
        }
    }

    /**
     * Creates JSON body generator for HTTP requests
     */
    private BodyGenerator createJsonBodyGenerator(Object object)
    {
        try {
            String json = objectMapper.writeValueAsString(object);
            return StaticBodyGenerator.createStaticBodyGenerator(json, UTF_8);
        }
        catch (Exception e) {
            throw new PolarisException("Failed to serialize request body", e);
        }
    }

    /**
     * Builds URI for API requests by simply concatenating base URI with path
     */
    private URI buildUri(String path)
    {
        return URI.create(config.getUri() + path);
    }

    /**
     * Encodes namespace for URL path
     */
    private String encodeNamespace(String namespace)
    {
        // TODO: Add proper URL encoding
        return namespace;
    }

    private static class ListGenericTablesResponse
    {
        private final List<TableIdentifierDto> identifiers;

        @JsonCreator
        public ListGenericTablesResponse(@JsonProperty("identifiers") List<TableIdentifierDto> identifiers)
        {
            this.identifiers = identifiers != null ? ImmutableList.copyOf(identifiers) : ImmutableList.of();
        }

        public List<PolarisTableIdentifier> toPolarisTableIdentifiers()
        {
            return identifiers.stream()
                    .map(TableIdentifierDto::toPolarisTableIdentifier)
                    .collect(toImmutableList());
        }
    }

    private static class TableIdentifierDto
    {
        private final List<String> namespace;
        private final String name;

        @JsonCreator
        public TableIdentifierDto(
                @JsonProperty("namespace") List<String> namespace,
                @JsonProperty("name") String name)
        {
            this.namespace = requireNonNull(namespace, "namespace is null");
            this.name = requireNonNull(name, "name is null");
        }

        public PolarisTableIdentifier toPolarisTableIdentifier()
        {
            return new PolarisTableIdentifier(String.join(".", namespace), name);
        }
    }

    private static class LoadGenericTableResponse
    {
        private final PolarisGenericTable table;

        @JsonCreator
        public LoadGenericTableResponse(@JsonProperty("table") PolarisGenericTable table)
        {
            this.table = requireNonNull(table, "table is null");
        }

        public PolarisGenericTable getTable()
        {
            return table;
        }
    }
}
