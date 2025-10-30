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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;

import static io.airlift.http.client.HttpUriBuilder.uriBuilder;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * REST client for Delta Sharing protocol.
 * Implements the official Delta Sharing REST API specification.
 */
public class DeltaSharingRestClient
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final HttpClient httpClient;
    private final DeltaSharingProfile profile;

    public DeltaSharingRestClient(HttpClient httpClient, DeltaSharingProfile profile)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.profile = requireNonNull(profile, "profile is null");
    }

    private HttpUriBuilder createBaseUriBuilder()
    {
        URI baseUri = URI.create(profile.getEndpoint());
        HttpUriBuilder uriBuilder = uriBuilder().scheme(baseUri.getScheme()).host(baseUri.getHost());
        if (baseUri.getPort() != -1) {
            uriBuilder.port(baseUri.getPort());
        }
        uriBuilder.appendPath(baseUri.getPath());
        return uriBuilder;
    }

    /**
     * List all shares available to the current user.
     * GET /shares
     */
    public SharesResponse listShares(Optional<Integer> maxResults, Optional<String> pageToken)
    {
        HttpUriBuilder uriBuilder = createBaseUriBuilder().appendPath("/shares");

        maxResults.ifPresent(max -> uriBuilder.addParameter("maxResults", String.valueOf(max)));
        pageToken.ifPresent(token -> uriBuilder.addParameter("pageToken", token));

        URI uri = uriBuilder.build();

        Request.Builder requestBuilder = prepareGet().setUri(uri).setHeader("User-Agent", "Trino-Delta-Sharing-Client");

        profile.getBearerToken().ifPresent(token -> requestBuilder.setHeader("Authorization", "Bearer " + token));

        Request request = requestBuilder.build();

        try {
            StringResponseHandler.StringResponse stringResponse = httpClient.execute(request, StringResponseHandler.createStringResponseHandler());
            String json = stringResponse.getBody();
            return OBJECT_MAPPER.readValue(json, SharesResponse.class);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list shares from Delta Sharing server: " + e.getMessage(), e);
        }
    }

    /**
     * List all schemas in a share.
     * GET /shares/{share}/schemas
     */
    public SchemasResponse listSchemas(String shareName, Optional<Integer> maxResults, Optional<String> pageToken)
    {
        requireNonNull(shareName, "shareName is null");

        HttpUriBuilder uriBuilder = createBaseUriBuilder().appendPath("/shares/" + shareName + "/schemas");

        maxResults.ifPresent(max -> uriBuilder.addParameter("maxResults", String.valueOf(max)));
        pageToken.ifPresent(token -> uriBuilder.addParameter("pageToken", token));

        URI uri = uriBuilder.build();

        Request.Builder requestBuilder = prepareGet().setUri(uri).setHeader("User-Agent", "Trino-Delta-Sharing-Client");

        profile.getBearerToken().ifPresent(token -> requestBuilder.setHeader("Authorization", "Bearer " + token));

        Request request = requestBuilder.build();

        try {
            StringResponseHandler.StringResponse stringResponse = httpClient.execute(request, StringResponseHandler.createStringResponseHandler());
            String json = stringResponse.getBody();

            JsonNode rootNode = OBJECT_MAPPER.readTree(json);
            JsonNode itemsNode = rootNode.get("items");

            ImmutableList.Builder<Schema> schemas = ImmutableList.builder();
            if (itemsNode != null && itemsNode.isArray()) {
                for (JsonNode schemaNode : itemsNode) {
                    String schemaName = schemaNode.get("name").asText();
                    schemas.add(new Schema(schemaName, shareName));
                }
            }

            String nextPageToken = rootNode.has("nextPageToken") ? rootNode.get("nextPageToken").asText() : null;
            return new SchemasResponse(schemas.build(), nextPageToken);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list schemas for share '" + shareName + "': " + e.getMessage(), e);
        }
    }

    /**
     * List all tables in a schema.
     * GET /shares/{share}/schemas/{schema}/tables
     */
    public TablesResponse listTables(String shareName, String schemaName, Optional<Integer> maxResults, Optional<String> pageToken)
    {
        requireNonNull(shareName, "shareName is null");
        requireNonNull(schemaName, "schemaName is null");

        HttpUriBuilder uriBuilder = createBaseUriBuilder().appendPath("/shares/" + shareName + "/schemas/" + schemaName + "/tables");

        maxResults.ifPresent(max -> uriBuilder.addParameter("maxResults", String.valueOf(max)));
        pageToken.ifPresent(token -> uriBuilder.addParameter("pageToken", token));

        URI uri = uriBuilder.build();

        Request.Builder requestBuilder = prepareGet().setUri(uri).setHeader("User-Agent", "Trino-Delta-Sharing-Client");

        profile.getBearerToken().ifPresent(token -> requestBuilder.setHeader("Authorization", "Bearer " + token));

        Request request = requestBuilder.build();

        try {
            StringResponseHandler.StringResponse stringResponse = httpClient.execute(request, StringResponseHandler.createStringResponseHandler());
            String json = stringResponse.getBody();

            JsonNode rootNode = OBJECT_MAPPER.readTree(json);
            JsonNode itemsNode = rootNode.get("items");

            ImmutableList.Builder<Table> tables = ImmutableList.builder();
            if (itemsNode != null && itemsNode.isArray()) {
                for (JsonNode tableNode : itemsNode) {
                    String tableName = tableNode.get("name").asText();
                    tables.add(new Table(tableName, shareName, schemaName));
                }
            }

            String nextPageToken = rootNode.has("nextPageToken") ? rootNode.get("nextPageToken").asText() : null;
            return new TablesResponse(tables.build(), nextPageToken);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list tables for share '" + shareName + "', schema '" + schemaName + "': " + e.getMessage(), e);
        }
    }

    /**
     * List all tables in a share across all schemas.
     * GET /shares/{share}/all-tables
     */
    public TablesResponse listAllTables(String shareName, Optional<Integer> maxResults, Optional<String> pageToken)
    {
        requireNonNull(shareName, "shareName is null");

        HttpUriBuilder uriBuilder = createBaseUriBuilder().appendPath("/shares/" + shareName + "/all-tables");

        maxResults.ifPresent(max -> uriBuilder.addParameter("maxResults", String.valueOf(max)));
        pageToken.ifPresent(token -> uriBuilder.addParameter("pageToken", token));

        URI uri = uriBuilder.build();

        Request.Builder requestBuilder = prepareGet().setUri(uri).setHeader("User-Agent", "Trino-Delta-Sharing-Client");

        profile.getBearerToken().ifPresent(token -> requestBuilder.setHeader("Authorization", "Bearer " + token));

        Request request = requestBuilder.build();

        try {
            StringResponseHandler.StringResponse stringResponse = httpClient.execute(request, StringResponseHandler.createStringResponseHandler());
            String json = stringResponse.getBody();

            JsonNode rootNode = OBJECT_MAPPER.readTree(json);
            JsonNode itemsNode = rootNode.get("items");

            ImmutableList.Builder<Table> tables = ImmutableList.builder();
            if (itemsNode != null && itemsNode.isArray()) {
                for (JsonNode tableNode : itemsNode) {
                    String tableName = tableNode.get("name").asText();
                    String schemaName = tableNode.get("schema").asText();
                    tables.add(new Table(tableName, shareName, schemaName));
                }
            }

            String nextPageToken = rootNode.has("nextPageToken") ? rootNode.get("nextPageToken").asText() : null;
            return new TablesResponse(tables.build(), nextPageToken);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list all tables for share '" + shareName + "': " + e.getMessage(), e);
        }
    }

    /**
     * Get the version of a table, optionally at a specific timestamp.
     * GET /shares/{share}/schemas/{schema}/tables/{table}/version
     */
    public long queryTableVersion(String shareName, String schemaName, String tableName, Optional<String> startingTimestamp)
    {
        requireNonNull(shareName, "shareName is null");
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");

        HttpUriBuilder uriBuilder = createBaseUriBuilder().appendPath("/shares/" + shareName + "/schemas/" + schemaName + "/tables/" + tableName + "/version");

        startingTimestamp.ifPresent(timestamp -> uriBuilder.addParameter("startingTimestamp", timestamp));

        URI uri = uriBuilder.build();

        Request.Builder requestBuilder = prepareGet().setUri(uri).setHeader("User-Agent", "Trino-Delta-Sharing-Client");

        profile.getBearerToken().ifPresent(token -> requestBuilder.setHeader("Authorization", "Bearer " + token));

        Request request = requestBuilder.build();

        try {
            StringResponseHandler.StringResponse stringResponse = httpClient.execute(request, StringResponseHandler.createStringResponseHandler());

            String versionHeader = stringResponse.getHeaders().get(HeaderName.of("delta-table-version")).stream().findFirst().orElse(null);
            if (versionHeader == null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Missing delta-table-version header in response");
            }

            return Long.parseLong(versionHeader);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid delta-table-version header: " + e.getMessage(), e);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to query table version for '" + shareName + "." + schemaName + "." + tableName + "': " + e.getMessage(), e);
        }
    }

    /**
     * Get table metadata including protocol and schema information.
     * POST /shares/{share}/schemas/{schema}/tables/{table}/metadata
     */
    public TableMetadata getTableMetadata(String shareName, String schemaName, String tableName)
    {
        requireNonNull(shareName, "shareName is null");
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");

        URI uri = createBaseUriBuilder().appendPath("/shares/" + shareName + "/schemas/" + schemaName + "/tables/" + tableName + "/metadata").build();

        Request.Builder requestBuilder = preparePost().setUri(uri).setHeader("User-Agent", "Trino-Delta-Sharing-Client").setHeader("Content-Type", "application/json");

        profile.getBearerToken().ifPresent(token -> requestBuilder.setHeader("Authorization", "Bearer " + token));

        Request request = requestBuilder.build();

        try {
            StringResponseHandler.StringResponse stringResponse = httpClient.execute(request, StringResponseHandler.createStringResponseHandler());
            String json = stringResponse.getBody();
            return parseTableMetadata(json);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get metadata for table '" + shareName + "." + schemaName + "." + tableName + "': " + e.getMessage(), e);
        }
    }

    private TableMetadata parseTableMetadata(String json)
            throws IOException
    {
        String[] lines = json.trim().split("\n");

        Protocol protocol = null;
        Metadata metadata = null;

        for (String line : lines) {
            JsonNode node = OBJECT_MAPPER.readTree(line);

            if (node.has("protocol")) {
                JsonNode protocolNode = node.get("protocol");
                protocol = new Protocol(protocolNode.get("minReaderVersion").asInt(), protocolNode.get("minWriterVersion").asInt());
            }
            else if (node.has("metaData")) {
                JsonNode metadataNode = node.get("metaData");
                metadata = new Metadata(metadataNode.get("id").asText(), metadataNode.has("name") ? metadataNode.get("name").asText() : null, metadataNode.has("description") ? metadataNode.get("description").asText() : null, metadataNode.get("schemaString").asText());
            }
            else if (node.has("metadata")) {
                JsonNode metadataNode = node.get("metadata");
                metadata = new Metadata(metadataNode.get("id").asText(), metadataNode.has("name") ? metadataNode.get("name").asText() : null, metadataNode.has("description") ? metadataNode.get("description").asText() : null, metadataNode.get("schemaString").asText());
            }
        }

        if (protocol == null || metadata == null) {
            System.out.println("Debug: Response JSON: " + json);
            System.out.println("Debug: Protocol found: " + (protocol != null));
            System.out.println("Debug: Metadata found: " + (metadata != null));
            throw new IOException("Invalid table metadata response: missing protocol or metadata");
        }

        String deltaPath = String.format("%s#%s.%s.%s", profile.getEndpoint(), "share", "schema", "table");

        return new TableMetadata(deltaPath, protocol, metadata);
    }

    public static class SharesResponse
    {
        private final List<Share> items;
        private final Optional<String> nextPageToken;

        @JsonCreator
        public SharesResponse(@JsonProperty("items") List<Share> items, @JsonProperty("nextPageToken") String nextPageToken)
        {
            this.items = ImmutableList.copyOf(requireNonNull(items, "items is null"));
            this.nextPageToken = Optional.ofNullable(nextPageToken);
        }

        public List<Share> getItems()
        {
            return items;
        }

        public Optional<String> getNextPageToken()
        {
            return nextPageToken;
        }
    }

    public static class SchemasResponse
    {
        private final List<Schema> items;
        private final Optional<String> nextPageToken;

        @JsonCreator
        public SchemasResponse(@JsonProperty("items") List<Schema> items, @JsonProperty("nextPageToken") String nextPageToken)
        {
            this.items = ImmutableList.copyOf(requireNonNull(items, "items is null"));
            this.nextPageToken = Optional.ofNullable(nextPageToken);
        }

        public List<Schema> getItems()
        {
            return items;
        }

        public Optional<String> getNextPageToken()
        {
            return nextPageToken;
        }
    }

    public static class TablesResponse
    {
        private final List<Table> items;
        private final Optional<String> nextPageToken;

        @JsonCreator
        public TablesResponse(@JsonProperty("items") List<Table> items, @JsonProperty("nextPageToken") String nextPageToken)
        {
            this.items = ImmutableList.copyOf(requireNonNull(items, "items is null"));
            this.nextPageToken = Optional.ofNullable(nextPageToken);
        }

        public List<Table> getItems()
        {
            return items;
        }

        public Optional<String> getNextPageToken()
        {
            return nextPageToken;
        }
    }

    public static class Share
    {
        private final String name;
        private final String id;

        @JsonCreator
        public Share(@JsonProperty("name") String name, @JsonProperty("id") String id)
        {
            this.name = requireNonNull(name, "name is null");
            this.id = id;
        }

        public String getName()
        {
            return name;
        }

        public Optional<String> getId()
        {
            return Optional.ofNullable(id);
        }
    }

    public static class Schema
    {
        private final String name;
        private final String share;

        @JsonCreator
        public Schema(@JsonProperty("name") String name, @JsonProperty("share") String share)
        {
            this.name = requireNonNull(name, "name is null");
            this.share = requireNonNull(share, "share is null");
        }

        public String getName()
        {
            return name;
        }

        public String getShare()
        {
            return share;
        }
    }

    public static class Table
    {
        private final String name;
        private final String share;
        private final String schema;

        @JsonCreator
        public Table(@JsonProperty("name") String name, @JsonProperty("share") String share, @JsonProperty("schema") String schema)
        {
            this.name = requireNonNull(name, "name is null");
            this.share = requireNonNull(share, "share is null");
            this.schema = requireNonNull(schema, "schema is null");
        }

        public String getName()
        {
            return name;
        }

        public String getShare()
        {
            return share;
        }

        public String getSchema()
        {
            return schema;
        }
    }

    public static class TableMetadata
    {
        private final String deltaPath;
        private final Protocol protocol;
        private final Metadata metadata;

        public TableMetadata(String deltaPath, Protocol protocol, Metadata metadata)
        {
            this.deltaPath = requireNonNull(deltaPath, "deltaPath is null");
            this.protocol = requireNonNull(protocol, "protocol is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        public String getDeltaPath()
        {
            return deltaPath;
        }

        public Protocol getProtocol()
        {
            return protocol;
        }

        public Metadata getMetadata()
        {
            return metadata;
        }
    }

    public static class Protocol
    {
        private final int minReaderVersion;
        private final int minWriterVersion;

        public Protocol(int minReaderVersion, int minWriterVersion)
        {
            this.minReaderVersion = minReaderVersion;
            this.minWriterVersion = minWriterVersion;
        }

        public int getMinReaderVersion()
        {
            return minReaderVersion;
        }

        public int getMinWriterVersion()
        {
            return minWriterVersion;
        }
    }

    public static class Metadata
    {
        private final String id;
        private final String name;
        private final String description;
        private final String schemaString;

        public Metadata(String id, String name, String description, String schemaString)
        {
            this.id = requireNonNull(id, "id is null");
            this.name = name;
            this.description = description;
            this.schemaString = requireNonNull(schemaString, "schemaString is null");
        }

        public String getId()
        {
            return id;
        }

        public Optional<String> getName()
        {
            return Optional.ofNullable(name);
        }

        public Optional<String> getDescription()
        {
            return Optional.ofNullable(description);
        }

        public String getSchemaString()
        {
            return schemaString;
        }
    }
}
