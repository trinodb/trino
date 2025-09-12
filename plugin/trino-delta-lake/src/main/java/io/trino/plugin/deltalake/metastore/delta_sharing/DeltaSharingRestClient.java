package io.trino.plugin.deltalake.metastore.delta_sharing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.trino.spi.TrinoException;
import jakarta.inject.Inject;

import javax.swing.text.html.Option;
import java.net.URI;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class DeltaSharingRestClient {
    private static final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient httpClient;
    private final DeltaSharingProfile profile;

    public DeltaSharingRestClient(HttpClient httpClient, DeltaSharingProfile profile) {
        this.httpClient = httpClient;
        this.profile = profile;
    }

    public static class Share {
        private final String name;

        @JsonCreator
        public Share(@JsonProperty("name") String name) {
            this.name = name;
        }
    }

    public static class Schema
    {
        private final String name;
        private final String share;

        @JsonCreator
        public Schema(@JsonProperty("name") String name, @JsonProperty("share") String share)
        {
            this.name = name;
            this.share = share;
        }
    }

    public static class Table
    {
        private final String name;
        private final String share;
        private final String schema;

        @JsonCreator
        public Table(
                @JsonProperty("name") String name,
                @JsonProperty("share") String share,
                @JsonProperty("schema") String schema)
        {
            this.name = name;
            this.share = share;
            this.schema = schema;
        }
    }

    public static class SharesResponse {
        private final List<Share> shares;
        private final Optional<String> nextPageToken;

        @JsonCreator
        public SharesResponse(
                @JsonProperty("items") List<Share> items,
                @JsonProperty("nextPageToken") String nextPageToken)
        {
            this.shares = ImmutableList.copyOf(items);
            this.nextPageToken = Optional.ofNullable(nextPageToken);
        }
    }

    public SharesResponse listShares(Optional<Integer> maxResults, Optional<String> pageToken) {
        HttpUriBuilder uriBuilder = HttpUriBuilder.uriBuilder().scheme("https").host(profile.getEndpoint()).appendPath("/shares");
        maxResults.ifPresent(max -> uriBuilder.addParameter("maxResults", String.valueOf(max)));
        pageToken.ifPresent(token -> uriBuilder.addParameter("pageToken", token));
        URI uri = uriBuilder.build();
        Request.Builder requestBuilder = Request.Builder.prepareGet().setUri(uri).setHeader("User-Agent", "Trino-Delta-Sharing-Client").setHeader("Authorization", "Bearer " + profile.getBearerToken());
        Request request = requestBuilder.build();
        try {
            StringResponseHandler.StringResponse response = httpClient.execute(request, StringResponseHandler.createStringResponseHandler());
            String json = response.getBody();
            return mapper.readValue(json, SharesResponse.class);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list shares from Delta Sharing server: " + e.getMessage(), e);
        }
    }

    public static class SchemasResponse {
        private final List<Schema> schemas;
        private final Optional<String> nextPageToken;

        @JsonCreator
        public SchemasResponse(
                @JsonProperty("items") List<Schema> items,
                @JsonProperty("nextPageToken") String nextPageToken) {
            this.schemas = ImmutableList.copyOf(items);
            this.nextPageToken = Optional.ofNullable(nextPageToken);
        }
    }

    public SchemasResponse listSchemas(String shareName, Optional<Integer> maxResults, Optional<String> pageToken) {
        HttpUriBuilder uriBuilder = HttpUriBuilder.uriBuilder()
                .scheme("https")
                .host(profile.getEndpoint())
                .appendPath("/shares/" + shareName + "/schemas");
        maxResults.ifPresent(max -> uriBuilder.addParameter("maxResults", String.valueOf(max)));
        pageToken.ifPresent(token -> uriBuilder.addParameter("pageToken", token));
        URI uri = uriBuilder.build();
        Request.Builder requestBuilder = Request.Builder.prepareGet().setUri(uri).setHeader("User-Agent", "Trino-Delta-Sharing-Client").setHeader("Authorization", "Bearer " + profile.getBearerToken());
        Request request = requestBuilder.build();
        try {
            StringResponseHandler.StringResponse response = httpClient.execute(request, StringResponseHandler.createStringResponseHandler());
            String json = response.getBody();
            JsonNode rootNode = mapper.readTree(json);
            JsonNode itemsNode = rootNode.get("items");
            ImmutableList.Builder<Schema> schemas = ImmutableList.builder();
            if (itemsNode != null && itemsNode.isArray()) {
                for (JsonNode schemaNode: itemsNode) {
                    String schemaName = schemaNode.get("name").asText();
                    schemas.add(new Schema(schemaName, shareName));
                }
            }
            String nextPageToken = rootNode.has("nextPageToken") ? rootNode.get("nextPageToken").asText() : null;
            return new SchemasResponse(schemas.build(), nextPageToken);
        } catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to list schemas for share '" + shareName + "': " + e.getMessage(), e);
        }
    }

    public static class TablesResponse
    {
        private final List<Table> tables;
        private final Optional<String> nextPageToken;

        @JsonCreator
        public TablesResponse(
                @JsonProperty("items") List<Table> items,
                @JsonProperty("nextPageToken") String nextPageToken)
        {
            this.tables = ImmutableList.copyOf(items);
            this.nextPageToken = Optional.ofNullable(nextPageToken);
        }
    }

    public TablesResponse listTables(String shareName, String schemaName, Optional<Integer> maxResults, Optional<String> pageToken) {
        HttpUriBuilder uriBuilder = HttpUriBuilder.uriBuilder()
                .scheme("https")
                .host(profile.getEndpoint())
                .appendPath("/shares/" + shareName + "/schemas/" + schemaName + "/tables");

        maxResults.ifPresent(max -> uriBuilder.addParameter("maxResults", String.valueOf(max)));
        pageToken.ifPresent(token -> uriBuilder.addParameter("pageToken", token));
        URI uri = uriBuilder.build();
        Request.Builder requestBuilder = Request.Builder.prepareGet().setUri(uri).setHeader("User-Agent", "Trino-Delta-Sharing-Client").setHeader("Authorization", "Bearer " + profile.getBearerToken());
        Request request = requestBuilder.build();
        try {
            StringResponseHandler.StringResponse stringResponse = httpClient.execute(request, StringResponseHandler.createStringResponseHandler());
            String json = stringResponse.getBody();
            JsonNode rootNode = mapper.readTree(json);
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
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "Failed to list tables for share '" + shareName + "', schema '" + schemaName + "': " + e.getMessage(), e);
        }
    }
}
