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
package io.trino.plugin.elasticsearch.client;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.json.JsonCodec;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.trino.plugin.elasticsearch.AwsSecurityConfig;
import io.trino.plugin.elasticsearch.ElasticsearchConfig;
import io.trino.plugin.elasticsearch.PasswordConfig;
import io.trino.spi.TrinoException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.plugin.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_CONNECTION_ERROR;
import static io.trino.plugin.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_INVALID_METADATA;
import static io.trino.plugin.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_INVALID_RESPONSE;
import static io.trino.plugin.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_QUERY_FAILURE;
import static io.trino.plugin.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_SSL_INITIALIZATION_FAILURE;
import static java.lang.StrictMath.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;

public class ElasticsearchClient
{
    private static final Logger LOG = Logger.get(ElasticsearchClient.class);

    private static final JsonCodec<SearchShardsResponse> SEARCH_SHARDS_RESPONSE_CODEC = jsonCodec(SearchShardsResponse.class);
    private static final JsonCodec<NodesResponse> NODES_RESPONSE_CODEC = jsonCodec(NodesResponse.class);
    private static final JsonCodec<CountResponse> COUNT_RESPONSE_CODEC = jsonCodec(CountResponse.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private static final Pattern ADDRESS_PATTERN = Pattern.compile("((?<cname>[^/]+)/)?(?<ip>.+):(?<port>\\d+)");
    private static final Set<String> NODE_ROLES = ImmutableSet.of("data", "data_content", "data_hot", "data_warm", "data_cold", "data_frozen");

    private final BackpressureRestHighLevelClient client;
    private final int scrollSize;
    private final Duration scrollTimeout;

    private final AtomicReference<Set<ElasticsearchNode>> nodes = new AtomicReference<>(ImmutableSet.of());
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("NodeRefresher"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final Duration refreshInterval;
    private final boolean tlsEnabled;
    private final boolean ignorePublishAddress;

    private final TimeStat searchStats = new TimeStat(MILLISECONDS);
    private final TimeStat nextPageStats = new TimeStat(MILLISECONDS);
    private final TimeStat countStats = new TimeStat(MILLISECONDS);
    private final TimeStat backpressureStats = new TimeStat(MILLISECONDS);

    @Inject
    public ElasticsearchClient(
            ElasticsearchConfig config,
            Optional<AwsSecurityConfig> awsSecurityConfig,
            Optional<PasswordConfig> passwordConfig)
    {
        client = createClient(config, awsSecurityConfig, passwordConfig, backpressureStats);

        this.ignorePublishAddress = config.isIgnorePublishAddress();
        this.scrollSize = config.getScrollSize();
        this.scrollTimeout = config.getScrollTimeout();
        this.refreshInterval = config.getNodeRefreshInterval();
        this.tlsEnabled = config.isTlsEnabled();
    }

    @PostConstruct
    public void initialize()
    {
        if (!started.getAndSet(true)) {
            // do the first refresh eagerly
            refreshNodes();

            executor.scheduleWithFixedDelay(this::refreshNodes, refreshInterval.toMillis(), refreshInterval.toMillis(), MILLISECONDS);
        }
    }

    @PreDestroy
    public void close()
            throws IOException
    {
        executor.shutdownNow();
        client.close();
    }

    private void refreshNodes()
    {
        // discover other nodes in the cluster and add them to the client
        try {
            Set<ElasticsearchNode> nodes = fetchNodes();

            HttpHost[] hosts = nodes.stream()
                    .map(ElasticsearchNode::getAddress)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(address -> HttpHost.create(format("%s://%s", tlsEnabled ? "https" : "http", address)))
                    .toArray(HttpHost[]::new);

            if (hosts.length > 0 && !ignorePublishAddress) {
                client.getLowLevelClient().setHosts(hosts);
            }

            this.nodes.set(nodes);
        }
        catch (Throwable e) {
            // Catch all exceptions here since throwing an exception from executor#scheduleWithFixedDelay method
            // suppresses all future scheduled invocations
            LOG.error(e, "Error refreshing nodes");
        }
    }

    private static BackpressureRestHighLevelClient createClient(
            ElasticsearchConfig config,
            Optional<AwsSecurityConfig> awsSecurityConfig,
            Optional<PasswordConfig> passwordConfig,
            TimeStat backpressureStats)
    {
        RestClientBuilder builder = RestClient.builder(
                config.getHosts().stream()
                        .map(httpHost -> new HttpHost(httpHost, config.getPort(), config.isTlsEnabled() ? "https" : "http"))
                        .toArray(HttpHost[]::new))
                .setMaxRetryTimeoutMillis(toIntExact(config.getMaxRetryTime().toMillis()));

        builder.setHttpClientConfigCallback(ignored -> {
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(toIntExact(config.getConnectTimeout().toMillis()))
                    .setSocketTimeout(toIntExact(config.getRequestTimeout().toMillis()))
                    .build();

            IOReactorConfig reactorConfig = IOReactorConfig.custom()
                    .setIoThreadCount(config.getHttpThreadCount())
                    .build();

            // the client builder passed to the call-back is configured to use system properties, which makes it
            // impossible to configure concurrency settings, so we need to build a new one from scratch
            HttpAsyncClientBuilder clientBuilder = HttpAsyncClientBuilder.create()
                    .setDefaultRequestConfig(requestConfig)
                    .setDefaultIOReactorConfig(reactorConfig)
                    .setMaxConnPerRoute(config.getMaxHttpConnections())
                    .setMaxConnTotal(config.getMaxHttpConnections());
            if (config.isTlsEnabled()) {
                buildSslContext(config.getKeystorePath(), config.getKeystorePassword(), config.getTrustStorePath(), config.getTruststorePassword())
                        .ifPresent(clientBuilder::setSSLContext);

                if (config.isVerifyHostnames()) {
                    clientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                }
            }

            passwordConfig.ifPresent(securityConfig -> {
                CredentialsProvider credentials = new BasicCredentialsProvider();
                credentials.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(securityConfig.getUser(), securityConfig.getPassword()));
                clientBuilder.setDefaultCredentialsProvider(credentials);
            });

            awsSecurityConfig.ifPresent(securityConfig -> clientBuilder.addInterceptorLast(new AwsRequestSigner(
                    securityConfig.getRegion(),
                    getAwsCredentialsProvider(securityConfig))));

            return clientBuilder;
        });

        return new BackpressureRestHighLevelClient(builder, config, backpressureStats);
    }

    private static AWSCredentialsProvider getAwsCredentialsProvider(AwsSecurityConfig config)
    {
        AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();

        if (config.getAccessKey().isPresent() && config.getSecretKey().isPresent()) {
            credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                    config.getAccessKey().get(),
                    config.getSecretKey().get()));
        }

        if (config.getIamRole().isPresent()) {
            STSAssumeRoleSessionCredentialsProvider.Builder credentialsProviderBuilder = new STSAssumeRoleSessionCredentialsProvider.Builder(config.getIamRole().get(), "trino-session")
                    .withStsClient(AWSSecurityTokenServiceClientBuilder.standard()
                            .withRegion(config.getRegion())
                            .withCredentials(credentialsProvider)
                            .build());
            config.getExternalId().ifPresent(credentialsProviderBuilder::withExternalId);
            credentialsProvider = credentialsProviderBuilder.build();
        }

        return credentialsProvider;
    }

    private static Optional<SSLContext> buildSslContext(
            Optional<File> keyStorePath,
            Optional<String> keyStorePassword,
            Optional<File> trustStorePath,
            Optional<String> trustStorePassword)
    {
        if (keyStorePath.isEmpty() && trustStorePath.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(createSSLContext(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword));
        }
        catch (GeneralSecurityException | IOException e) {
            throw new TrinoException(ELASTICSEARCH_SSL_INITIALIZATION_FAILURE, e);
        }
    }

    private Set<ElasticsearchNode> fetchNodes()
    {
        NodesResponse nodesResponse = doRequest("/_nodes/http", NODES_RESPONSE_CODEC::fromJson);

        ImmutableSet.Builder<ElasticsearchNode> result = ImmutableSet.builder();
        for (Map.Entry<String, NodesResponse.Node> entry : nodesResponse.getNodes().entrySet()) {
            String nodeId = entry.getKey();
            NodesResponse.Node node = entry.getValue();

            if (!Sets.intersection(node.getRoles(), NODE_ROLES).isEmpty()) {
                Optional<String> address = node.getAddress()
                        .flatMap(ElasticsearchClient::extractAddress);

                result.add(new ElasticsearchNode(nodeId, address));
            }
        }

        return result.build();
    }

    public Set<ElasticsearchNode> getNodes()
    {
        return nodes.get();
    }

    public List<Shard> getSearchShards(String index)
    {
        Map<String, ElasticsearchNode> nodeById = getNodes().stream()
                .collect(toImmutableMap(ElasticsearchNode::getId, Function.identity()));

        SearchShardsResponse shardsResponse = doRequest(format("/%s/_search_shards", index), SEARCH_SHARDS_RESPONSE_CODEC::fromJson);

        ImmutableList.Builder<Shard> shards = ImmutableList.builder();
        List<ElasticsearchNode> nodes = ImmutableList.copyOf(nodeById.values());

        for (List<SearchShardsResponse.Shard> shardGroup : shardsResponse.getShardGroups()) {
            Optional<SearchShardsResponse.Shard> candidate = shardGroup.stream()
                    .filter(shard -> shard.getNode() != null && nodeById.containsKey(shard.getNode()))
                    .min(this::shardPreference);

            SearchShardsResponse.Shard chosen;
            ElasticsearchNode node;
            if (candidate.isEmpty()) {
                // pick an arbitrary shard with and assign to an arbitrary node
                chosen = shardGroup.stream()
                        .min(this::shardPreference)
                        .get();
                node = nodes.get(chosen.getShard() % nodes.size());
            }
            else {
                chosen = candidate.get();
                node = nodeById.get(chosen.getNode());
            }

            shards.add(new Shard(chosen.getIndex(), chosen.getShard(), node.getAddress()));
        }

        return shards.build();
    }

    private int shardPreference(SearchShardsResponse.Shard left, SearchShardsResponse.Shard right)
    {
        // Favor non-primary shards
        if (left.isPrimary() == right.isPrimary()) {
            return 0;
        }

        return left.isPrimary() ? 1 : -1;
    }

    public boolean indexExists(String index)
    {
        String path = format("/%s/_mappings", index);

        try {
            Response response = client.getLowLevelClient()
                    .performRequest("GET", path);

            return response.getStatusLine().getStatusCode() == 200;
        }
        catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                return false;
            }
            throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
    }

    public List<String> getIndexes()
    {
        return doRequest("/_cat/indices?h=index,docs.count,docs.deleted&format=json&s=index:asc", body -> {
            try {
                ImmutableList.Builder<String> result = ImmutableList.builder();
                JsonNode root = OBJECT_MAPPER.readTree(body);
                for (int i = 0; i < root.size(); i++) {
                    String index = root.get(i).get("index").asText();
                    // make sure the index has mappings we can use to derive the schema
                    int docsCount = root.get(i).get("docs.count").asInt();
                    int deletedDocsCount = root.get(i).get("docs.deleted").asInt();
                    if (docsCount == 0 && deletedDocsCount == 0) {
                        // without documents, the index won't have any dynamic mappings, but maybe there are some explicit ones
                        if (getIndexMetadata(index).getSchema().getFields().isEmpty()) {
                            continue;
                        }
                    }
                    result.add(index);
                }
                return result.build();
            }
            catch (IOException e) {
                throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
            }
        });
    }

    public Map<String, List<String>> getAliases()
    {
        return doRequest("/_aliases", body -> {
            try {
                ImmutableMap.Builder<String, List<String>> result = ImmutableMap.builder();
                JsonNode root = OBJECT_MAPPER.readTree(body);

                Iterator<Map.Entry<String, JsonNode>> elements = root.fields();
                while (elements.hasNext()) {
                    Map.Entry<String, JsonNode> element = elements.next();
                    JsonNode aliases = element.getValue().get("aliases");
                    Iterator<String> aliasNames = aliases.fieldNames();
                    if (aliasNames.hasNext()) {
                        result.put(element.getKey(), ImmutableList.copyOf(aliasNames));
                    }
                }
                return result.buildOrThrow();
            }
            catch (IOException e) {
                throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
            }
        });
    }

    public IndexMetadata getIndexMetadata(String index)
    {
        String path = format("/%s/_mappings", index);

        return doRequest(path, body -> {
            try {
                JsonNode mappings = OBJECT_MAPPER.readTree(body)
                        .elements().next()
                        .get("mappings");

                if (!mappings.elements().hasNext()) {
                    return new IndexMetadata(new IndexMetadata.ObjectType(ImmutableList.of()));
                }
                if (!mappings.has("properties")) {
                    // Older versions of ElasticSearch supported multiple "type" mappings
                    // for a given index. Newer versions support only one and don't
                    // expose it in the document. Here we skip it if it's present.
                    mappings = mappings.elements().next();

                    if (!mappings.has("properties")) {
                        return new IndexMetadata(new IndexMetadata.ObjectType(ImmutableList.of()));
                    }
                }

                JsonNode metaNode = nullSafeNode(mappings, "_meta");

                JsonNode metaProperties = nullSafeNode(metaNode, "trino");

                //stay backwards compatible with _meta.presto namespace for meta properties for some releases
                if (metaProperties.isNull()) {
                    metaProperties = nullSafeNode(metaNode, "presto");
                }

                return new IndexMetadata(parseType(mappings.get("properties"), metaProperties));
            }
            catch (IOException e) {
                throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
            }
        });
    }

    private IndexMetadata.ObjectType parseType(JsonNode properties, JsonNode metaProperties)
    {
        Iterator<Map.Entry<String, JsonNode>> entries = properties.fields();

        ImmutableList.Builder<IndexMetadata.Field> result = ImmutableList.builder();
        while (entries.hasNext()) {
            Map.Entry<String, JsonNode> field = entries.next();

            String name = field.getKey();
            JsonNode value = field.getValue();

            //default type is object
            String type = "object";
            if (value.has("type")) {
                type = value.get("type").asText();
            }
            JsonNode metaNode = nullSafeNode(metaProperties, name);
            boolean isArray = !metaNode.isNull() && metaNode.has("isArray") && metaNode.get("isArray").asBoolean();
            boolean asRawJson = !metaNode.isNull() && metaNode.has("asRawJson") && metaNode.get("asRawJson").asBoolean();

            // While it is possible to handle isArray and asRawJson in the same column by creating a ARRAY(VARCHAR) type, we chose not to take
            // this route, as it will likely lead to confusion in dealing with array syntax in Trino and potentially nested array and other
            // syntax when parsing the raw json.
            if (isArray && asRawJson) {
                throw new TrinoException(ELASTICSEARCH_INVALID_METADATA,
                        format("A column, (%s) cannot be declared as a Trino array and also be rendered as json.", name));
            }

            switch (type) {
                case "date":
                    List<String> formats = ImmutableList.of();
                    if (value.has("format")) {
                        formats = Arrays.asList(value.get("format").asText().split("\\|\\|"));
                    }
                    result.add(new IndexMetadata.Field(asRawJson, isArray, name, new IndexMetadata.DateTimeType(formats)));
                    break;
                case "scaled_float":
                    result.add(new IndexMetadata.Field(asRawJson, isArray, name, new IndexMetadata.ScaledFloatType(value.get("scaling_factor").asDouble())));
                    break;
                case "nested":
                case "object":
                    if (value.has("properties")) {
                        result.add(new IndexMetadata.Field(asRawJson, isArray, name, parseType(value.get("properties"), metaNode)));
                    }
                    else {
                        LOG.debug("Ignoring empty object field: %s", name);
                    }
                    break;

                default:
                    result.add(new IndexMetadata.Field(asRawJson, isArray, name, new IndexMetadata.PrimitiveType(type)));
            }
        }

        return new IndexMetadata.ObjectType(result.build());
    }

    private JsonNode nullSafeNode(JsonNode jsonNode, String name)
    {
        if (jsonNode == null || jsonNode.isNull() || jsonNode.get(name) == null) {
            return NullNode.getInstance();
        }
        return jsonNode.get(name);
    }

    public String executeQuery(String index, String query)
    {
        String path = format("/%s/_search", index);

        Response response;
        try {
            response = client.getLowLevelClient()
                    .performRequest(
                            "GET",
                            path,
                            ImmutableMap.of(),
                            new ByteArrayEntity(query.getBytes(UTF_8)),
                            new BasicHeader("Content-Type", "application/json"),
                            new BasicHeader("Accept-Encoding", "application/json"));
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }

        String body;
        try {
            body = EntityUtils.toString(response.getEntity());
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
        }

        return body;
    }

    public SearchResponse beginSearch(String index, int shard, QueryBuilder query, Optional<List<String>> fields, List<String> documentFields, Optional<String> sort, OptionalLong limit)
    {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource()
                .query(query);

        if (limit.isPresent() && limit.getAsLong() < scrollSize) {
            // Safe to cast it to int because scrollSize is int.
            sourceBuilder.size(toIntExact(limit.getAsLong()));
        }
        else {
            sourceBuilder.size(scrollSize);
        }

        sort.ifPresent(sourceBuilder::sort);

        fields.ifPresent(values -> {
            if (values.isEmpty()) {
                sourceBuilder.fetchSource(false);
            }
            else {
                sourceBuilder.fetchSource(values.toArray(new String[0]), null);
            }
        });
        documentFields.forEach(sourceBuilder::docValueField);

        LOG.debug("Begin search: %s:%s, query: %s", index, shard, sourceBuilder);

        SearchRequest request = new SearchRequest(index)
                .searchType(QUERY_THEN_FETCH)
                .preference("_shards:" + shard)
                .scroll(new TimeValue(scrollTimeout.toMillis()))
                .source(sourceBuilder);

        long start = System.nanoTime();
        try {
            return client.search(request);
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
        catch (ElasticsearchStatusException e) {
            Throwable[] suppressed = e.getSuppressed();
            if (suppressed.length > 0) {
                Throwable cause = suppressed[0];
                if (cause instanceof ResponseException) {
                    throw propagate((ResponseException) cause);
                }
            }

            throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
        finally {
            searchStats.add(Duration.nanosSince(start));
        }
    }

    public SearchResponse nextPage(String scrollId)
    {
        LOG.debug("Next page: %s", scrollId);

        SearchScrollRequest request = new SearchScrollRequest(scrollId)
                .scroll(new TimeValue(scrollTimeout.toMillis()));

        long start = System.nanoTime();
        try {
            return client.searchScroll(request);
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
        finally {
            nextPageStats.add(Duration.nanosSince(start));
        }
    }

    public long count(String index, int shard, QueryBuilder query)
    {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource()
                .query(query);

        LOG.debug("Count: %s:%s, query: %s", index, shard, sourceBuilder);

        long start = System.nanoTime();
        try {
            Response response;
            try {
                response = client.getLowLevelClient()
                        .performRequest(
                                "GET",
                                format("/%s/_count?preference=_shards:%s", index, shard),
                                ImmutableMap.of(),
                                new StringEntity(sourceBuilder.toString()),
                                new BasicHeader("Content-Type", "application/json"));
            }
            catch (ResponseException e) {
                throw propagate(e);
            }
            catch (IOException e) {
                throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
            }

            try {
                return COUNT_RESPONSE_CODEC.fromJson(EntityUtils.toByteArray(response.getEntity()))
                        .getCount();
            }
            catch (IOException e) {
                throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
            }
        }
        finally {
            countStats.add(Duration.nanosSince(start));
        }
    }

    public void clearScroll(String scrollId)
    {
        ClearScrollRequest request = new ClearScrollRequest();
        request.addScrollId(scrollId);
        try {
            client.clearScroll(request);
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
    }

    @Managed
    @Nested
    public TimeStat getSearchStats()
    {
        return searchStats;
    }

    @Managed
    @Nested
    public TimeStat getNextPageStats()
    {
        return nextPageStats;
    }

    @Managed
    @Nested
    public TimeStat getCountStats()
    {
        return countStats;
    }

    @Managed
    @Nested
    public TimeStat getBackpressureStats()
    {
        return backpressureStats;
    }

    private <T> T doRequest(String path, ResponseHandler<T> handler)
    {
        checkArgument(path.startsWith("/"), "path must be an absolute path");

        Response response;
        try {
            response = client.getLowLevelClient()
                    .performRequest("GET", path);
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }

        String body;
        try {
            body = EntityUtils.toString(response.getEntity());
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
        }

        return handler.process(body);
    }

    private static TrinoException propagate(ResponseException exception)
    {
        HttpEntity entity = exception.getResponse().getEntity();

        if (entity != null && entity.getContentType() != null) {
            try {
                JsonNode reason = OBJECT_MAPPER.readTree(entity.getContent()).path("error")
                        .path("root_cause")
                        .path(0)
                        .path("reason");

                if (!reason.isMissingNode()) {
                    throw new TrinoException(ELASTICSEARCH_QUERY_FAILURE, reason.asText(), exception);
                }
            }
            catch (IOException e) {
                TrinoException result = new TrinoException(ELASTICSEARCH_QUERY_FAILURE, exception);
                result.addSuppressed(e);
                throw result;
            }
        }

        throw new TrinoException(ELASTICSEARCH_QUERY_FAILURE, exception);
    }

    @VisibleForTesting
    static Optional<String> extractAddress(String address)
    {
        Matcher matcher = ADDRESS_PATTERN.matcher(address);

        if (!matcher.matches()) {
            return Optional.empty();
        }

        String cname = matcher.group("cname");
        String ip = matcher.group("ip");
        String port = matcher.group("port");

        if (cname != null) {
            return Optional.of(cname + ":" + port);
        }

        return Optional.of(ip + ":" + port);
    }

    private interface ResponseHandler<T>
    {
        T process(String body);
    }
}
