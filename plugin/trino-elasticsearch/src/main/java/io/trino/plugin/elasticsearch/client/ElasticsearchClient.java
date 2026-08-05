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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonMapperProvider;
import io.airlift.log.Logger;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.trino.plugin.elasticsearch.AwsSecurityConfig;
import io.trino.plugin.elasticsearch.ElasticsearchConfig;
import io.trino.plugin.elasticsearch.PasswordConfig;
import io.trino.spi.TrinoException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
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
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalDouble;
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

public class ElasticsearchClient
{
    private static final Logger LOG = Logger.get(ElasticsearchClient.class);

    private static final JsonCodec<SearchShardsResponse> SEARCH_SHARDS_RESPONSE_CODEC = jsonCodec(SearchShardsResponse.class);
    private static final JsonCodec<NodesResponse> NODES_RESPONSE_CODEC = jsonCodec(NodesResponse.class);
    private static final JsonCodec<CountResponse> COUNT_RESPONSE_CODEC = jsonCodec(CountResponse.class);
    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider().get();
    private static final JsonNodeFactory JSON = JsonNodeFactory.instance;

    private static final Pattern ADDRESS_PATTERN = Pattern.compile("((?<cname>[^/]+)/)?(?<ip>.+):(?<port>\\d+)");
    private static final Set<String> NODE_ROLES = ImmutableSet.of("data", "data_content", "data_hot", "data_warm", "data_cold", "data_frozen");

    private final BackpressureRestClient client;
    private final int scrollSize;
    private final Duration scrollTimeout;
    private final int statisticsRequestTimeoutMillis;

    private final AtomicReference<Set<ElasticsearchNode>> nodes = new AtomicReference<>(ImmutableSet.of());
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("NodeRefresher"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final Duration refreshInterval;
    private final boolean tlsEnabled;
    private final boolean ignorePublishAddress;
    private final boolean keywordSubfieldPushdownWithIgnoreAbove;

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
        this.statisticsRequestTimeoutMillis = toIntExact(config.getStatisticsRequestTimeout().toMillis());
        this.refreshInterval = config.getNodeRefreshInterval();
        this.tlsEnabled = config.isTlsEnabled();
        this.keywordSubfieldPushdownWithIgnoreAbove = config.isKeywordSubfieldPushdownWithIgnoreAbove();
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
                    .map(ElasticsearchNode::address)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(address -> HttpHost.create(format("%s://%s", tlsEnabled ? "https" : "http", address)))
                    .toArray(HttpHost[]::new);

            if (hosts.length > 0 && !ignorePublishAddress) {
                client.setHosts(hosts);
            }

            this.nodes.set(nodes);
        }
        catch (Throwable e) {
            // Catch all exceptions here since throwing an exception from executor#scheduleWithFixedDelay method
            // suppresses all future scheduled invocations
            LOG.error(e, "Error refreshing nodes");
        }
    }

    private static BackpressureRestClient createClient(
            ElasticsearchConfig config,
            Optional<AwsSecurityConfig> awsSecurityConfig,
            Optional<PasswordConfig> passwordConfig,
            TimeStat backpressureStats)
    {
        RestClientBuilder builder = RestClient.builder(
                config.getHosts().stream()
                        .map(httpHost -> new HttpHost(httpHost, config.getPort(), config.isTlsEnabled() ? "https" : "http"))
                        .toArray(HttpHost[]::new));

        builder.setHttpClientConfigCallback(_ -> {
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

                if (!config.isVerifyHostnames()) {
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

        return new BackpressureRestClient(builder.build(), config, backpressureStats);
    }

    private static AwsCredentialsProvider getAwsCredentialsProvider(AwsSecurityConfig config)
    {
        AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

        if (config.getAccessKey().isPresent() && config.getSecretKey().isPresent()) {
            credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(
                    config.getAccessKey().get(),
                    config.getSecretKey().get()));
        }

        if (config.getIamRole().isPresent()) {
            StsAssumeRoleCredentialsProvider.Builder credentialsProviderBuilder = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(StsClient.builder()
                            .region(Region.of(config.getRegion()))
                            .credentialsProvider(credentialsProvider)
                            .build())
                    .refreshRequest(request -> {
                        request
                                .roleArn(config.getIamRole().get())
                                .roleSessionName("trino-session");
                        config.getExternalId().ifPresent(request::externalId);
                    });
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
        for (Entry<String, NodesResponse.Node> entry : nodesResponse.getNodes().entrySet()) {
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
                .collect(toImmutableMap(ElasticsearchNode::id, Function.identity()));

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

            shards.add(new Shard(chosen.getIndex(), chosen.getShard(), node.address()));
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
            Response response = client.performRequest("GET", path);

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
                JsonNode root = JSON_MAPPER.readTree(body);
                for (int i = 0; i < root.size(); i++) {
                    String index = root.get(i).get("index").asText();
                    // make sure the index has mappings we can use to derive the schema
                    int docsCount = root.get(i).get("docs.count").asInt();
                    int deletedDocsCount = root.get(i).get("docs.deleted").asInt();
                    if (docsCount == 0 && deletedDocsCount == 0) {
                        try {
                            // without documents, the index won't have any dynamic mappings, but maybe there are some explicit ones
                            if (getIndexMetadata(index).schema().fields().isEmpty()) {
                                continue;
                            }
                        }
                        catch (TrinoException e) {
                            if (e.getErrorCode().equals(ELASTICSEARCH_INVALID_METADATA.toErrorCode())) {
                                continue;
                            }
                            if (e.getCause() instanceof ResponseException cause && cause.getResponse().getStatusLine().getStatusCode() == 404) {
                                continue;
                            }
                            throw e;
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
                JsonNode root = JSON_MAPPER.readTree(body);

                for (Entry<String, JsonNode> element : root.properties()) {
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

    public boolean isKeywordSubfieldPushdownWithIgnoreAbove()
    {
        return keywordSubfieldPushdownWithIgnoreAbove;
    }

    public IndexMetadata getIndexMetadata(String index)
    {
        return getIndexMetadata(index, keywordSubfieldPushdownWithIgnoreAbove);
    }

    public IndexMetadata getIndexMetadata(String index, boolean useBoundedKeyword)
    {
        String path = format("/%s/_mappings", index);

        return doRequest(path, body -> {
            try {
                JsonNode mappings = JSON_MAPPER.readTree(body)
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

                // stay backwards compatible with _meta.presto namespace for meta properties for some releases
                if (metaProperties.isNull()) {
                    metaProperties = nullSafeNode(metaNode, "presto");
                }

                return new IndexMetadata(parseType(mappings.get("properties"), metaProperties, useBoundedKeyword));
            }
            catch (IOException e) {
                throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
            }
        });
    }

    private IndexMetadata.ObjectType parseType(JsonNode properties, JsonNode metaProperties, boolean useBoundedKeyword)
    {
        ImmutableList.Builder<IndexMetadata.Field> result = ImmutableList.builder();
        for (Entry<String, JsonNode> field : properties.properties()) {
            String name = field.getKey();
            JsonNode value = field.getValue();

            // default type is object
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
                throw new TrinoException(
                        ELASTICSEARCH_INVALID_METADATA,
                        format("A column, (%s) cannot be declared as a Trino array and also be rendered as json.", name));
            }

            switch (type) {
                case "date" -> {
                    List<String> formats = ImmutableList.of();
                    if (value.has("format")) {
                        formats = Arrays.asList(value.get("format").asText().split("\\|\\|"));
                    }
                    result.add(new IndexMetadata.Field(asRawJson, isArray, name, new IndexMetadata.DateTimeType(formats)));
                }
                case "scaled_float" -> result.add(new IndexMetadata.Field(asRawJson, isArray, name, new IndexMetadata.ScaledFloatType(value.get("scaling_factor").asDouble())));
                case "nested", "object" -> {
                    if (value.has("properties")) {
                        result.add(new IndexMetadata.Field(asRawJson, isArray, name, parseType(value.get("properties"), metaNode, useBoundedKeyword)));
                    }
                    else {
                        LOG.debug("Ignoring empty object field: %s", name);
                    }
                }
                default -> {
                    IndexMetadata.PrimitiveType primitiveType;
                    if (type.equals("text")) {
                        primitiveType = new IndexMetadata.PrimitiveType(type, keywordSubfield(value, useBoundedKeyword));
                    }
                    else if (type.equals("keyword") && value.get("normalizer") != null) {
                        // A keyword field with a normalizer stores a rewritten (for example lowercased) value, not the
                        // verbatim source, so it is not exact for predicate/sort/aggregation pushdown; treat it as analyzed text
                        primitiveType = new IndexMetadata.PrimitiveType("text");
                    }
                    else {
                        primitiveType = new IndexMetadata.PrimitiveType(type);
                    }
                    result.add(new IndexMetadata.Field(asRawJson, isArray, name, primitiveType));
                }
            }
        }

        return new IndexMetadata.ObjectType(result.build());
    }

    @VisibleForTesting
    static Optional<String> keywordSubfield(JsonNode fieldNode, boolean pushDownWithIgnoreAbove)
    {
        JsonNode fields = fieldNode.get("fields");
        if (fields == null) {
            return Optional.empty();
        }
        Optional<String> boundedSubfield = Optional.empty();
        for (Entry<String, JsonNode> subfield : fields.properties()) {
            JsonNode subfieldNode = subfield.getValue();
            JsonNode type = subfieldNode.get("type");
            if (type == null || !"keyword".equals(type.asText())) {
                continue;
            }
            // A normalizer rewrites the value (for example lowercasing), so the sub-field is not a verbatim copy of the
            // source and is unsafe for exact predicate/sort/aggregation pushdown
            if (subfieldNode.get("normalizer") != null) {
                continue;
            }
            // A keyword sub-field without ignore_above indexes every value and is always safe for pushdown, so prefer it
            if (subfieldNode.get("ignore_above") == null) {
                return Optional.of(subfield.getKey());
            }
            // A sub-field with ignore_above may drop longer values from its index; it is only used, as a fallback,
            // when the operator explicitly opts in and no unbounded keyword sub-field is available.
            if (boundedSubfield.isEmpty()) {
                boundedSubfield = Optional.of(subfield.getKey());
            }
        }
        if (pushDownWithIgnoreAbove) {
            return boundedSubfield;
        }
        return Optional.empty();
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
            response = client.performRequest(
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

    public SearchResult beginSearch(String index, int shard, JsonNode query, Optional<List<String>> fields, List<String> documentFields, List<JsonNode> sort, OptionalLong limit)
    {
        ObjectNode searchBody = JSON.objectNode();
        searchBody.set("query", query);

        int size;
        if (limit.isPresent() && limit.getAsLong() < scrollSize) {
            size = toIntExact(limit.getAsLong());
        }
        else {
            size = scrollSize;
        }
        searchBody.put("size", size);

        if (!sort.isEmpty()) {
            ArrayNode sortArray = JSON.arrayNode();
            sort.forEach(sortArray::add);
            searchBody.set("sort", sortArray);
        }

        fields.ifPresent(values -> {
            if (values.isEmpty()) {
                searchBody.put("_source", false);
            }
            else {
                ArrayNode includes = JSON.arrayNode();
                values.forEach(includes::add);
                searchBody.set("_source", JSON.objectNode().set("includes", includes));
            }
        });

        if (!documentFields.isEmpty()) {
            ArrayNode docFields = JSON.arrayNode();
            documentFields.forEach(docFields::add);
            searchBody.set("docvalue_fields", docFields);
        }

        LOG.debug("Begin search: %s:%s, query: %s", index, shard, searchBody);

        String endpoint = format("/%s/_search?preference=_shards:%s&scroll=%sms", index, shard, scrollTimeout.toMillis());

        long start = System.nanoTime();
        try {
            Response response = client.performRequest(
                    "POST",
                    endpoint,
                    ImmutableMap.of(),
                    new StringEntity(searchBody.toString(), UTF_8),
                    new BasicHeader("Content-Type", "application/json"));
            return parseSearchResponse(response);
        }
        catch (ResponseException e) {
            throw propagate(e);
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
        finally {
            searchStats.add(Duration.nanosSince(start));
        }
    }

    public SearchResult nextPage(String scrollId)
    {
        LOG.debug("Next page: %s", scrollId);

        ObjectNode scrollBody = JSON.objectNode();
        scrollBody.put("scroll", scrollTimeout.toMillis() + "ms");
        scrollBody.put("scroll_id", scrollId);

        long start = System.nanoTime();
        try {
            Response response = client.performRequest(
                    "POST",
                    "/_search/scroll",
                    ImmutableMap.of(),
                    new StringEntity(scrollBody.toString(), UTF_8),
                    new BasicHeader("Content-Type", "application/json"));
            return parseSearchResponse(response);
        }
        catch (ResponseException e) {
            throw propagate(e);
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
        finally {
            nextPageStats.add(Duration.nanosSince(start));
        }
    }

    public long count(String index, int shard, JsonNode query)
    {
        ObjectNode countBody = JSON.objectNode();
        countBody.set("query", query);

        LOG.debug("Count: %s:%s, query: %s", index, shard, countBody);

        long start = System.nanoTime();
        try {
            Response response;
            try {
                response = client.performRequest(
                        "GET",
                        format("/%s/_count?preference=_shards:%s", index, shard),
                        ImmutableMap.of(),
                        new StringEntity(countBody.toString(), UTF_8),
                        new BasicHeader("Content-Type", "application/json"));
            }
            catch (ResponseException e) {
                throw propagate(e);
            }
            catch (IOException e) {
                throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
            }

            try {
                return COUNT_RESPONSE_CODEC.fromJson(response.getEntity().getContent())
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

    public IndexStatistics getIndexStatistics(String index, JsonNode query, List<String> fields, Set<String> rangeFields, boolean includeCardinality)
    {
        ObjectNode body = JSON.objectNode();
        body.put("size", 0);
        body.put("track_total_hits", true);
        body.set("query", query);

        ObjectNode aggregations = JSON.objectNode();
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            String name = "f" + i;
            // cardinality (HyperLogLog) is the expensive per-document aggregation; skip it on large indices
            if (includeCardinality) {
                aggregations.set(name + "_card", fieldAggregation("cardinality", field));
            }
            aggregations.set(name + "_count", fieldAggregation("value_count", field));
            if (rangeFields.contains(field)) {
                aggregations.set(name + "_min", fieldAggregation("min", field));
                aggregations.set(name + "_max", fieldAggregation("max", field));
            }
        }
        if (!aggregations.isEmpty()) {
            body.set("aggs", aggregations);
        }

        Response response;
        try {
            response = client.performRequestWithTimeout(
                    "POST",
                    format("/%s/_search", index),
                    ImmutableMap.of(),
                    new StringEntity(body.toString(), UTF_8),
                    statisticsRequestTimeoutMillis,
                    new BasicHeader("Content-Type", "application/json"));
        }
        catch (ResponseException e) {
            throw propagate(e);
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }

        try {
            JsonNode root = JSON_MAPPER.readTree(response.getEntity().getContent());
            long documentCount = root.path("hits").path("total").path("value").asLong();

            JsonNode aggregationResults = root.path("aggregations");
            ImmutableMap.Builder<String, FieldStatistics> statistics = ImmutableMap.builder();
            for (int i = 0; i < fields.size(); i++) {
                String name = "f" + i;
                statistics.put(fields.get(i), new FieldStatistics(
                        longValue(aggregationResults.path(name + "_card")),
                        longValue(aggregationResults.path(name + "_count")),
                        doubleValue(aggregationResults.path(name + "_min")),
                        doubleValue(aggregationResults.path(name + "_max"))));
            }
            return new IndexStatistics(documentCount, statistics.buildOrThrow());
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
        }
    }

    public JsonNode aggregate(String index, JsonNode body)
    {
        Response response;
        try {
            response = client.performRequest(
                    "POST",
                    format("/%s/_search", index),
                    ImmutableMap.of(),
                    new StringEntity(body.toString(), UTF_8),
                    new BasicHeader("Content-Type", "application/json"));
        }
        catch (ResponseException e) {
            throw propagate(e);
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }

        try {
            return JSON_MAPPER.readTree(response.getEntity().getContent());
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
        }
    }

    private static ObjectNode fieldAggregation(String aggregation, String field)
    {
        return JSON.objectNode().set(aggregation, JSON.objectNode().put("field", field));
    }

    private static OptionalLong longValue(JsonNode aggregation)
    {
        JsonNode value = aggregation.path("value");
        return value.isNumber() ? OptionalLong.of(value.asLong()) : OptionalLong.empty();
    }

    private static OptionalDouble doubleValue(JsonNode aggregation)
    {
        JsonNode value = aggregation.path("value");
        return value.isNumber() ? OptionalDouble.of(value.asDouble()) : OptionalDouble.empty();
    }

    public void clearScroll(String scrollId)
    {
        ObjectNode body = JSON.objectNode();
        ArrayNode scrollIds = JSON.arrayNode();
        scrollIds.add(scrollId);
        body.set("scroll_id", scrollIds);

        try {
            client.performRequest(
                    "DELETE",
                    "/_search/scroll",
                    ImmutableMap.of(),
                    new StringEntity(body.toString(), UTF_8),
                    new BasicHeader("Content-Type", "application/json"));
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }
    }

    private SearchResult parseSearchResponse(Response response)
    {
        try {
            String responseBody = EntityUtils.toString(response.getEntity());
            JsonNode root = JSON_MAPPER.readTree(responseBody);

            Optional<String> scrollId = Optional.ofNullable(root.get("_scroll_id"))
                    .map(JsonNode::asText);

            JsonNode hitsNode = root.get("hits").get("hits");
            ImmutableList.Builder<SearchDocument> hits = ImmutableList.builder();

            for (JsonNode hitNode : hitsNode) {
                String id = hitNode.get("_id").asText();
                float score = hitNode.has("_score") && !hitNode.get("_score").isNull()
                        ? hitNode.get("_score").floatValue()
                        : Float.NaN;

                JsonNode sourceNode = hitNode.get("_source");
                String sourceAsString = sourceNode != null ? sourceNode.toString() : "{}";
                long sourceLength = sourceAsString.getBytes(UTF_8).length;
                @SuppressWarnings("unchecked")
                Map<String, Object> sourceAsMap = sourceNode != null
                        ? JSON_MAPPER.convertValue(sourceNode, Map.class)
                        : ImmutableMap.of();

                Map<String, List<Object>> fields = ImmutableMap.of();
                if (hitNode.has("fields")) {
                    Map<String, List<Object>> fieldsMap = new LinkedHashMap<>();
                    for (Entry<String, JsonNode> fieldEntry : hitNode.get("fields").properties()) {
                        List<Object> values = new ArrayList<>();
                        for (JsonNode valueNode : fieldEntry.getValue()) {
                            values.add(nodeToValue(valueNode));
                        }
                        fieldsMap.put(fieldEntry.getKey(), values);
                    }
                    fields = ImmutableMap.copyOf(fieldsMap);
                }

                hits.add(new SearchDocument(id, score, sourceAsMap, sourceAsString, sourceLength, fields));
            }

            return new SearchResult(scrollId, hits.build());
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
        }
    }

    private static Object nodeToValue(JsonNode node)
    {
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isNumber()) {
            return node.numberValue();
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isNull()) {
            return null;
        }
        throw new IllegalArgumentException("Unsupported node type: " + node.getClass());
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
            response = client.performRequest("GET", path);
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_CONNECTION_ERROR, e);
        }

        try (InputStream stream = response.getEntity().getContent()) {
            return handler.process(stream);
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
        }
    }

    private static TrinoException propagate(ResponseException exception)
    {
        HttpEntity entity = exception.getResponse().getEntity();

        if (entity != null && entity.getContentType() != null) {
            try {
                JsonNode reason = JSON_MAPPER.readTree(entity.getContent()).path("error")
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
        T process(InputStream stream);
    }
}
