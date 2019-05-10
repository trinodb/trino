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

package io.prestosql.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.google.common.io.Closer;
import com.google.common.net.HostAndPort;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.units.Duration;
import io.prestosql.spi.PrestoException;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMKEY_PASSWORD;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH;
import static com.floragunn.searchguard.ssl.util.SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_CONNECTION_ERROR;
import static io.prestosql.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_CORRUPTED_INDICES_METADATA;
import static io.prestosql.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_CORRUPTED_MAPPING_METADATA;
import static io.prestosql.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_CORRUPTED_SHARDS_METADATA;
import static io.prestosql.elasticsearch.RetryDriver.retry;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_MULT_CHOICE;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ElasticsearchMetadataFetcher
{
    private final ObjectMapperProvider objectMapperProvider;

    private final Duration requestTimeout;
    private final int maxAttempts;
    private final Duration maxRetryTime;
    private final ElasticsearchConnectorConfig config;

    private final ElasticsearchClientWrapper httpClientWrapper;
    private final ElasticsearchClientWrapper nativeClientWrapper;

    @Inject
    public ElasticsearchMetadataFetcher(ElasticsearchConnectorConfig config, ObjectMapperProvider objectMapperProvider)
    {
        this.objectMapperProvider = objectMapperProvider;

        this.requestTimeout = config.getRequestTimeout();
        this.maxAttempts = config.getMaxRequestRetries();
        this.maxRetryTime = config.getMaxRetryTime();
        this.config = config;

        this.nativeClientWrapper = new ElasticsearchNativeClientWrapper();
        this.httpClientWrapper = new ElasticsearchHttpClientWrapper();
    }

    private static void closeClient(Closeable client)
    {
        try {
            if (client != null) {
                client.close();
            }
        }
        catch (IOException ex) {
            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, format("Failed to close connection"), ex);
        }
    }

    static TransportClient createNativeTransportClient(ElasticsearchConnectorConfig config, String host, int port)
    {
        return createNativeTransportClient(config, host, port, Optional.empty());
    }

    static TransportClient createNativeTransportClient(ElasticsearchConnectorConfig config, String host, int port, Optional<String> clusterName)
    {
        TransportAddress address;
        try {
            address = new TransportAddress(InetAddress.getByName(host), port);
        }
        catch (Exception e) {
            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, format("Failed to establish connection to search node: %s:%s", host, port), e);
        }

        Settings settings;
        Settings.Builder builder = Settings.builder();
        if (clusterName.isPresent()) {
            builder.put("cluster.name", clusterName.get());
        }
        else {
            builder.put("client.transport.ignore_cluster_name", true);
        }
        switch (config.getCertificateFormat()) {
            case PEM:
                settings = builder
                        .put(SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH, config.getPemcertFilepath())
                        .put(SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH, config.getPemkeyFilepath())
                        .put(SEARCHGUARD_SSL_TRANSPORT_PEMKEY_PASSWORD, config.getPemkeyPassword())
                        .put(SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH, config.getPemtrustedcasFilepath())
                        .put(SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
                        .build();
                return new PreBuiltTransportClient(settings, SearchGuardSSLPlugin.class).addTransportAddress(address);
            case JKS:
                settings = builder
                        .put(SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, config.getKeystoreFilepath())
                        .put(SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH, config.getTruststoreFilepath())
                        .put(SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD, config.getKeystorePassword())
                        .put(SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD, config.getTruststorePassword())
                        .put(SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
                        .build();
                return new PreBuiltTransportClient(settings, SearchGuardSSLPlugin.class).addTransportAddress(address);
            default:
                settings = builder.build();
                return new PreBuiltTransportClient(settings).addTransportAddress(address);
        }
    }

    /**
     * Get the shards given <i>index</i> in cluster defined by <i>table</i>.
     *
     * @param index
     * @param table
     * @return
     */
    public List<ElasticsearchShard> fetchShards(String index, ElasticsearchTableDescription table)
    {
        checkArgument(index != null, "index is null");
        checkArgument(table != null, "table is null");
        return clientWrapper(table).fetchShards(index, table);
    }

    /**
     * Fetch all indices have prefix given in <i>table</i>.
     *
     * @param table
     * @return
     */
    public List<String> fetchIndices(ElasticsearchTableDescription table)
    {
        checkArgument(table != null, "table is null");
        return clientWrapper(table).fetchIndices(table);
    }

    /**
     * Fetch mapping for <i>index</i> in cluster defined by <i>table</i>.
     *
     * @param index
     * @param table
     * @return
     */
    public JsonNode fetchMapping(String index, ElasticsearchTableDescription table)
    {
        checkArgument(index != null, "index is null");
        checkArgument(table != null, "table is null");
        return clientWrapper(table).fetchMapping(index, table);
    }

    private ElasticsearchClientWrapper clientWrapper(ElasticsearchTableDescription table)
    {
        return (table.isHttpPort().isPresent() && table.isHttpPort().get()) ? httpClientWrapper : nativeClientWrapper;
    }

    @PreDestroy
    public void tearDown()
    {
        try (Closer closer = Closer.create()) {
            if (httpClientWrapper != null) {
                closer.register(httpClientWrapper);
            }
            if (nativeClientWrapper != null) {
                closer.register(nativeClientWrapper);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Common interface for native/http Elasticsearch clients
     */
    private interface ElasticsearchClientWrapper
            extends Closeable
    {
        List<ElasticsearchShard> fetchShards(String index, ElasticsearchTableDescription table);

        List<String> fetchIndices(ElasticsearchTableDescription table);

        JsonNode fetchMapping(String index, ElasticsearchTableDescription table);
    }

    private static class ClusterClientKey
    {
        private final String clusterName;
        private final String host;
        private final int port;
        private final Optional<String> pathPrefix;
        private final Optional<Map<String, String>> headers;

        private ClusterClientKey(String clusterName, String host, int port, Optional<String> pathPrefix, Optional<Map<String, String>> headers)
        {
            this.clusterName = requireNonNull(clusterName, "clusterName is null");
            this.host = requireNonNull(host, "host is null");
            this.port = port;
            this.pathPrefix = requireNonNull(pathPrefix, "pathPrefix is null");
            this.headers = requireNonNull(headers, "headers is null");
        }

        public static ClusterClientKey from(ElasticsearchTableDescription tableDescription)
        {
            return new ClusterClientKey(tableDescription.getClusterName(), tableDescription.getHost(), tableDescription.getPort(),
                    tableDescription.getPathPrefix(), tableDescription.getHeaders());
        }

        public String getClusterName()
        {
            return clusterName;
        }

        public String getHost()
        {
            return host;
        }

        public int getPort()
        {
            return port;
        }

        public Optional<String> getPathPrefix()
        {
            return pathPrefix;
        }

        public Optional<Map<String, String>> getHeaders()
        {
            return headers;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (!(o instanceof ClusterClientKey)) {
                return false;
            }

            ClusterClientKey that = (ClusterClientKey) o;
            return port == that.port && Objects.equals(clusterName, that.clusterName) &&
                    Objects.equals(host, that.host) && Objects.equals(pathPrefix, that.pathPrefix)
                    && Objects.equals(headers, that.headers);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(clusterName, host, port, pathPrefix);
        }

        @Override
        public String toString()
        {
            return new StringBuilder()
                    .append("cluster=" + clusterName)
                    .append(", host=" + host)
                    .append(", port=" + port)
                    .append(", pathPrefix=" + pathPrefix)
                    .append(", headers=" + headers)
                    .toString();
        }
    }

    private class ElasticsearchHttpClientWrapper
            implements ElasticsearchClientWrapper
    {
        private final LoadingCache<ClusterClientKey, RestHighLevelClient> httpClientsCache;

        public ElasticsearchHttpClientWrapper()
        {
            this.httpClientsCache = CacheBuilder.newBuilder()
                    .expireAfterAccess(30, MINUTES)
                    .maximumSize(50)
                    .removalListener((RemovalNotification<ClusterClientKey, RestHighLevelClient> notification) -> closeClient(notification.getValue()))
                    .build(CacheLoader.from(this::createHttpConnection));
        }

        @Override
        public List<ElasticsearchShard> fetchShards(String index, ElasticsearchTableDescription table)
        {
            String requestPath = format("%s/_search_shards", index);

            RestHighLevelClient highLevelClient = httpClientsCache.getUnchecked(ClusterClientKey.from(table));
            JsonNode rootNode = performGetRequest(highLevelClient.getLowLevelClient(), requestPath, ELASTICSEARCH_CORRUPTED_SHARDS_METADATA);

            List<ElasticsearchShard> shards = new ArrayList<>();

            JsonNode nodesJson = rootNode.get("nodes");
            JsonNode shardsJson = rootNode.get("shards");

            for (int iShard = 0; iShard < shardsJson.size(); iShard++) {
                JsonNode shardReplicasJson = shardsJson.get(iShard);
                JsonNode shardJsonChosen = selectShardReplica(shardReplicasJson);
                shards.add(convertToElasticsearchShard(shardJsonChosen, nodesJson, index, table.getType()));
            }

            return shards;
        }

        @Override
        public List<String> fetchIndices(ElasticsearchTableDescription table)
        {
            RestHighLevelClient highLevelClient = httpClientsCache.getUnchecked(ClusterClientKey.from(table));
            String requestPath = format("_cat/indices/%s*?format=json", table.getIndex());
            JsonNode rootNode = performGetRequest(highLevelClient.getLowLevelClient(), requestPath, ELASTICSEARCH_CORRUPTED_INDICES_METADATA);

            List<String> indices = new ArrayList<>();
            for (JsonNode index : rootNode) {
                indices.add(index.get("index").asText());
            }

            return indices;
        }

        @Override
        public JsonNode fetchMapping(String index, ElasticsearchTableDescription table)
        {
            String indexType = table.getType();

            RestHighLevelClient highLevelClient = httpClientsCache.getUnchecked(ClusterClientKey.from(table));
            String requestPath = format("%s/%s/_mapping", index, indexType);
            JsonNode rootJson = performGetRequest(highLevelClient.getLowLevelClient(), requestPath, ELASTICSEARCH_CORRUPTED_MAPPING_METADATA);

            if (rootJson.fields().hasNext()) {
                JsonNode mappings = rootJson.fields().next().getValue();
                if (mappings.has("mappings") && mappings.get("mappings").has(indexType)) {
                    return mappings.get("mappings").get(indexType);
                }
            }

            throw new PrestoException(ELASTICSEARCH_CORRUPTED_MAPPING_METADATA,
                    format("Mappings not found. RequestPath: %s. Returned mappings: %s", indexType, requestPath, rootJson));
        }

        @Override
        public void close()
        {
            httpClientsCache.invalidateAll();
        }

        private RestHighLevelClient createHttpConnection(ClusterClientKey clusterClientKey)
        {
            try {
                RestClientBuilder clientBuilder = RestClient.builder(new HttpHost(clusterClientKey.getHost(), clusterClientKey.getPort(), "http"));
                if (clusterClientKey.getPathPrefix().isPresent()) {
                    clientBuilder.setPathPrefix(clusterClientKey.getPathPrefix().get());
                }

                if (clusterClientKey.getHeaders().isPresent()) {
                    Header[] headers = clusterClientKey.getHeaders().get().entrySet().stream()
                            .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                            .toArray(Header[]::new);

                    clientBuilder.setDefaultHeaders(headers);
                }

                clientBuilder.setMaxRetryTimeoutMillis((int) maxRetryTime.toMillis());

                return new RestHighLevelClient(clientBuilder);
            }
            catch (Exception e) {
                throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, format("Failed to establish connection: %s", clusterClientKey), e);
            }
        }

        private JsonNode performGetRequest(RestClient client, String path, ElasticsearchErrorCode errorCode)
        {
            Response response;

            try {
                // the RestHighLevelClient already has retry mechanism
                response = client.performRequest("GET", path);
                validateRestClientResponse(response);
            }
            catch (Exception e) {
                throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, format("Request '%s' failed", path), e);
            }

            try {
                String responseJson = EntityUtils.toString(response.getEntity());
                return objectMapperProvider.get().readTree(responseJson);
            }
            catch (IOException e) {
                throw new PrestoException(errorCode, path, e);
            }
        }

        private JsonNode selectShardReplica(JsonNode shardReplicasJson)
        {
            // if there is only one shard replica choose it, otherwise select randomly from one of the shard replicas
            // TODO: improve this to not select the primary replica shard
            return shardReplicasJson.get(new Random().nextInt(shardReplicasJson.size()));
        }

        private ElasticsearchShard convertToElasticsearchShard(JsonNode shardJson, JsonNode nodesJson, String index, String type)
        {
            JsonNode nodeNameJson = shardJson.get("node");
            String nodeName = nodeNameJson.asText();

            JsonNode nodeJson = nodesJson.get(nodeName);
            JsonNode transportJson = nodeJson.get("transport_address");
            HostAndPort hostAndPort = HostAndPort.fromString(transportJson.asText());

            return new ElasticsearchShard(
                    index,
                    type,
                    shardJson.get("shard").asInt(),
                    hostAndPort.getHost(),
                    hostAndPort.getPort());
        }

        private void validateRestClientResponse(Response response)
                throws IOException
        {
            int status = response.getStatusLine().getStatusCode();
            if (status >= HTTP_OK && status < HTTP_MULT_CHOICE) {
                return;
            }

            throw new IOException(format("Status code '%s', message '%s'", status, response.getStatusLine().getReasonPhrase()));
        }
    }

    private class ElasticsearchNativeClientWrapper
            implements ElasticsearchClientWrapper
    {
        private final LoadingCache<ClusterClientKey, Client> nativeClientsCache;

        public ElasticsearchNativeClientWrapper()
        {
            this.nativeClientsCache = CacheBuilder.newBuilder()
                    .expireAfterAccess(30, MINUTES)
                    .maximumSize(50)
                    .removalListener((RemovalNotification<ClusterClientKey, Client> notification) -> closeClient(notification.getValue()))
                    .build(CacheLoader.from(this::createNativeConnection));
        }

        @Override
        public List<ElasticsearchShard> fetchShards(String index, ElasticsearchTableDescription table)
        {
            ClusterClientKey clientKey = ClusterClientKey.from(table);

            Client client = nativeClientsCache.getUnchecked(clientKey);

            ClusterSearchShardsResponse response = performNativeRequest("getShards: " + index, clientKey,
                    () -> client.admin()
                            .cluster()
                            .searchShards(new ClusterSearchShardsRequest(index))
                            .actionGet(requestTimeout.toMillis()));

            DiscoveryNode[] nodes = response.getNodes();
            return Arrays.stream(response.getGroups())
                    .map(group -> {
                        int nodeIndex = group.getShardId().getId() % nodes.length;
                        return new ElasticsearchShard(
                                index,
                                table.getType(),
                                group.getShardId().getId(),
                                nodes[nodeIndex].getHostName(),
                                nodes[nodeIndex].getAddress().getPort());
                    })
                    .collect(Collectors.toList());
        }

        @Override
        public List<String> fetchIndices(ElasticsearchTableDescription table)
        {
            String indexPrefix = table.getIndex();

            ClusterClientKey clientKey = ClusterClientKey.from(table);

            Client client = nativeClientsCache.getUnchecked(clientKey);

            String[] indices = performNativeRequest("getIndices", clientKey,
                    () -> client.admin()
                            .indices()
                            .getIndex(new GetIndexRequest())
                            .actionGet(requestTimeout.toMillis())
                            .getIndices());

            return Arrays.stream(indices)
                    .filter(index -> index.startsWith(indexPrefix))
                    .collect(toImmutableList());
        }

        @Override
        public JsonNode fetchMapping(String index, ElasticsearchTableDescription table)
        {
            ClusterClientKey clientKey = ClusterClientKey.from(table);
            GetMappingsRequest mappingsRequest = new GetMappingsRequest()
                    .types(table.getType())
                    .indices(index);

            Client client = nativeClientsCache.getUnchecked(clientKey);
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings =
                    performNativeRequest("getMappings: " + index, clientKey,
                            () -> client.admin()
                                    .indices()
                                    .getMappings(mappingsRequest)
                                    .actionGet(requestTimeout.toMillis())
                                    .getMappings());

            try {
                MappingMetaData mappingMetaData = mappings.get(index).get(table.getType());
                return objectMapperProvider.get().readTree(mappingMetaData.source().uncompressed());
            }
            catch (IOException e) {
                throw new PrestoException(ELASTICSEARCH_CORRUPTED_MAPPING_METADATA, e);
            }
        }

        @Override
        public void close()
        {
            nativeClientsCache.invalidateAll();
        }

        private Client createNativeConnection(ClusterClientKey clusterClientKey)
        {
            return createNativeTransportClient(config, clusterClientKey.getHost(), clusterClientKey.getPort(), Optional.of(clusterClientKey.getClusterName()));
        }

        private <T> T performNativeRequest(String callMethod, ClusterClientKey clientKey, Callable<T> callable)
        {
            try {
                return retry()
                        .maxAttempts(maxAttempts)
                        .exponentialBackoff(maxRetryTime)
                        .run(callMethod, callable);
            }
            catch (Exception e) {
                throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, format("Elasticsearch connection failed: request=%s, %s", callMethod, clientKey), e);
            }
        }
    }
}
