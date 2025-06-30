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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.json.JsonCodec;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * REST client for Apache Polaris catalog API.
 *
 * This client:
 * - Uses Trino HttpClient for Generic Table operations with matching OAuth2 authentication
 */
public class PolarisRestClient
{
    private static final JsonCodec<Map<String, String>> AUTH_CODEC = mapJsonCodec(String.class, String.class);
    private static final JsonCodec<LoadGenericTableResponse> LOAD_GENERIC_TABLE_RESPONSE_CODEC = jsonCodec(LoadGenericTableResponse.class);
    private static final JsonCodec<ListGenericTablesResponse> LIST_TABLES_CODEC = jsonCodec(ListGenericTablesResponse.class);

    private final HttpClient httpClient;
    private final PolarisMetastoreConfig config;
    private final SecurityProperties securityProperties;
    private final ObjectMapper objectMapper;

    @Inject
    public PolarisRestClient(
            @ForPolarisClient HttpClient httpClient,
            PolarisMetastoreConfig config,
            SecurityProperties securityProperties,
            ObjectMapper objectMapper)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.config = requireNonNull(config, "config is null");
        this.securityProperties = requireNonNull(securityProperties, "securityProperties is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    // AUTHENTICATION & HTTP UTILITIES

    /**
     * Gets authentication headers by reusing the same OAuth2 credentials as RESTSessionCatalog
     * This ensures identical authentication behavior between Iceberg and Generic table operations
     */
    private Map<String, String> getAuthHeaders()
    {
        try {
            // Extract OAuth2 credentials exactly like TrinoIcebergRestCatalogFactory does
            Map<String, String> securityProps = securityProperties.get();

            Map<String, String> credentials = Maps.filterKeys(securityProps,
                    key -> Set.of(OAuth2Properties.TOKEN, OAuth2Properties.CREDENTIAL).contains(key));

            // If we have a direct token, use it
            if (credentials.containsKey(OAuth2Properties.TOKEN)) {
                return ImmutableMap.of("Authorization", "Bearer " + credentials.get(OAuth2Properties.TOKEN));
            }

            // If we have credentials, perform OAuth2 token exchange like RESTSessionCatalog does
            if (credentials.containsKey(OAuth2Properties.CREDENTIAL)) {
                String token = performOAuth2TokenExchange(credentials.get(OAuth2Properties.CREDENTIAL));
                return ImmutableMap.of("Authorization", "Bearer " + token);
            }

            // No authentication credentials found, return empty headers
            return ImmutableMap.of();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new PolarisException("Failed to get authentication headers", e);
        }
    }

    /**
     * Performs OAuth2 token exchange using the same flow as RESTSessionCatalog
     */
    private String performOAuth2TokenExchange(String credential)
    {
        try {
            // Parse credential (format: "client_id:client_secret")
            String[] parts = credential.split(":", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid credential format. Expected 'client_id:client_secret'");
            }
            String clientId = parts[0];
            String clientSecret = parts[1];

            // Build OAuth2 token request
            Map<String, String> tokenRequest = ImmutableMap.of(
                    "grant_type", "client_credentials",
                    "client_id", clientId,
                    "client_secret", clientSecret,
                    "scope", securityProperties.get().getOrDefault(OAuth2Properties.SCOPE, "PRINCIPAL_ROLE:ALL"));

            // Create form-encoded body
            String body = tokenRequest.entrySet().stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                    .collect(joining("&"));

            // Make token request
            URI tokenUri = URI.create(securityProperties.get().get(OAuth2Properties.OAUTH2_SERVER_URI));
            Request request = preparePost()
                    .setUri(tokenUri)
                    .setHeader("Content-Type", "application/x-www-form-urlencoded")
                    .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(body, UTF_8))
                    .build();

            // Execute token request
            try {
                Map<String, String> response = httpClient.execute(request, createJsonResponseHandler(AUTH_CODEC));
                return response.get("access_token");
            }
            catch (UnexpectedResponseException e) {
                throw new PolarisException("OAuth2 token exchange failed with status: " + e.getStatusCode(), e);
            }
            catch (Exception e) {
                throw new PolarisException("OAuth2 token exchange failed", e);
            }
        }
        catch (Exception e) {
            throw new PolarisException("OAuth2 token exchange failed", e);
        }
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

        try {
            return execute(request, createJsonResponseHandler(LIST_TABLES_CODEC)).toPolarisTableIdentifiers();
        }
        catch (UnexpectedResponseException e) {
            throw new PolarisException("Failed to list generic tables: " + e.getStatusCode(), e);
        }
        catch (Exception e) {
            throw new PolarisException("Failed to list generic tables", e);
        }
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

        try {
            LoadGenericTableResponse response = execute(request, createJsonResponseHandler(LOAD_GENERIC_TABLE_RESPONSE_CODEC));
            return response.getTable();
        }
        catch (UnexpectedResponseException e) {
            if (e.getStatusCode() == 404) {
                throw new TableNotFoundException(new SchemaTableName(namespaceName, tableName));
            }
            throw new PolarisException("Failed to load generic table: " + tableName + " (status: " + e.getStatusCode() + ")", e);
        }
        catch (Exception e) {
            throw new PolarisException("Failed to load generic table: " + tableName, e);
        }
    }

    /**
     * Creates a Generic table using Polaris-specific API
     */
    public void createGenericTable(String databaseName, PolarisGenericTable genericTable)
    {
        URI uri = buildUri("/polaris/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(databaseName) + "/generic-tables");

        CreateGenericTableRequest request = new CreateGenericTableRequest(
                genericTable.name(),
                genericTable.format(),
                genericTable.baseLocation().orElse(null),
                genericTable.doc().orElse(null),
                genericTable.properties());

        Request httpRequest = preparePost()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .addHeader("Content-Type", "application/json")
                .setBodyGenerator(createJsonBodyGenerator(request))
                .build();

        execute(httpRequest, new ResponseHandler<Void, RuntimeException>()
        {
            @Override
            public Void handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to create generic table: " + genericTable.name(), exception);
            }

            @Override
            public Void handle(Request request, Response response)
            {
                if (response.getStatusCode() == 409) {
                    throw new PolarisAlreadyExistsException("Generic table already exists: " + genericTable.name());
                }
                if (response.getStatusCode() != 200 && response.getStatusCode() != 201) {
                    throw new PolarisException("Failed to create generic table: " + response.getStatusCode());
                }
                return null;
            }
        });
    }

    /**
     * Drops a Generic table using Polaris-specific API
     */
    public void dropGenericTable(String databaseName, String tableName)
    {
        URI uri = buildUri("/polaris/v1/" + config.getPrefix() + "/namespaces/" + encodeNamespace(databaseName) + "/generic-tables/" + tableName);

        Request request = prepareDelete()
                .setUri(uri)
                .addHeaders(buildHeaders(getAuthHeaders()))
                .build();

        execute(request, new ResponseHandler<Void, RuntimeException>()
        {
            @Override
            public Void handleException(Request request, Exception exception)
            {
                throw new PolarisException("Failed to drop generic table: " + tableName, exception);
            }

            @Override
            public Void handle(Request request, Response response)
            {
                if (response.getStatusCode() == 404) {
                    throw new PolarisNotFoundException("Generic table not found: " + tableName);
                }
                if (response.getStatusCode() != 204) {
                    throw new PolarisException("Failed to drop generic table: " + response.getStatusCode());
                }
                return null;
            }
        });
    }

    // HELPER METHODS

    /**
     * Builds headers for HTTP requests
     */
    private com.google.common.collect.Multimap<String, String> buildHeaders(Map<String, String> headers)
    {
        return ImmutableListMultimap.copyOf(headers.entrySet());
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
        try {
            return java.net.URLEncoder.encode(namespace, UTF_8);
        }
        catch (Exception e) {
            // Fallback to simple replacement for common cases
            return namespace.replace(".", "%2E");
        }
    }

    public static class ListGenericTablesResponse
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

    public static class TableIdentifierDto
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

    public static class LoadGenericTableResponse
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

    public static class CreateGenericTableRequest
    {
        private final String name;
        private final String format;
        private final String baseLocation;
        private final String doc;
        private final Map<String, String> properties;

        public CreateGenericTableRequest(String name, String format, String baseLocation, String doc, Map<String, String> properties)
        {
            this.name = requireNonNull(name, "name is null");
            this.format = requireNonNull(format, "format is null");
            this.baseLocation = baseLocation; // Optional
            this.doc = doc; // Optional
            this.properties = properties != null ? ImmutableMap.copyOf(properties) : ImmutableMap.of();
        }

        @JsonProperty("name")
        public String getName()
        {
            return name;
        }

        @JsonProperty("format")
        public String getFormat()
        {
            return format;
        }

        @JsonProperty("base-location")
        public String getBaseLocation()
        {
            return baseLocation;
        }

        @JsonProperty("doc")
        public String getDoc()
        {
            return doc;
        }

        @JsonProperty("properties")
        public Map<String, String> getProperties()
        {
            return properties;
        }
    }
}
