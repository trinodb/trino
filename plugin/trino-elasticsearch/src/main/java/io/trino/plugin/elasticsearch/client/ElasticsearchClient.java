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
import io.trino.plugin.elasticsearch.expression.TopN.TopNSortItem;
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
import static java.util.stream.Collectors.joining;

public class ElasticsearchClient
{
    private static final Logger LOG = Logger.get(ElasticsearchClient.class);

    private static final JsonCodec<SearchShardsResponse> SEARCH_SHARDS_RESPONSE_CODEC = jsonCodec(SearchShardsResponse.class);
    private static final JsonCodec<NodesResponse> NODES_RESPONSE_CODEC = jsonCodec(NodesResponse.class);
    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider().get();
    private static final JsonNodeFactory JSON = JsonNodeFactory.instance;

    private static final Pattern ADDRESS_PATTERN = Pattern.compile("((?<cname>[^/]+)/)?(?<ip>.+):(?<port>\\d+)");
    private static final Set<String> NODE_ROLES = ImmutableSet.of("data", "data_content", "data_hot", "data_warm", "data_cold", "data_frozen");

    private final BackpressureRestClient client;
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

    public IndexMetadata getIndexMetadata(String index)
    {
        JsonNode indices = doRequest(format("/%s?flat_settings=true&filter_path=*.mappings,*.settings", index), body -> {
            try {
                return JSON_MAPPER.readTree(body);
            }
            catch (IOException e) {
                throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
            }
        });
        Map<String, Set<String>> ignoredMalformedFields = ImmutableMap.of();
        if (indices.properties().stream().anyMatch(entry -> requiresFieldMappingDefaults(entry.getValue()))) {
            ignoredMalformedFields = fetchIgnoredMalformedFields(index);
        }
        ImmutableList.Builder<IndexMetadata.ObjectType> schemas = ImmutableList.builder();
        for (Entry<String, JsonNode> indexEntry : indices.properties().stream().sorted(Entry.comparingByKey()).toList()) {
            Optional<Set<String>> ignoredFields = Optional.of(ImmutableSet.of());
            if (requiresFieldMappingDefaults(indexEntry.getValue())) {
                ignoredFields = Optional.ofNullable(ignoredMalformedFields.get(indexEntry.getKey()));
            }
            schemas.add(parseIndexMetadata(
                    nullSafeNode(indexEntry.getValue(), "mappings"),
                    ignoredFields));
        }
        List<IndexMetadata.ObjectType> indexSchemas = schemas.build();
        if (indexSchemas.isEmpty()) {
            return new IndexMetadata(new IndexMetadata.ObjectType(ImmutableList.of()));
        }
        return new IndexMetadata(mergeMappingSafety(indexSchemas.getFirst(), indexSchemas.subList(1, indexSchemas.size())));
    }

    private static boolean requiresFieldMappingDefaults(JsonNode indexMetadata)
    {
        JsonNode settings = indexMetadata.path("settings");
        return !settings.isObject()
                || (settings.has("index.mapping.ignore_malformed")
                && !settings.path("index.mapping.ignore_malformed").asText().equals("false"));
    }

    private Map<String, Set<String>> fetchIgnoredMalformedFields(String index)
    {
        try {
            return doRequest(format("/%s/_mapping/field/*?include_defaults=true&filter_path=*.mappings.*.full_name,*.mappings.*.mapping.*.ignore_malformed", index), body -> {
                try {
                    ImmutableMap.Builder<String, Set<String>> result = ImmutableMap.builder();
                    for (Entry<String, JsonNode> indexEntry : JSON_MAPPER.readTree(body).properties()) {
                        ImmutableSet.Builder<String> fields = ImmutableSet.builder();
                        for (JsonNode field : indexEntry.getValue().path("mappings")) {
                            for (JsonNode mapping : field.path("mapping")) {
                                if (mapping.path("ignore_malformed").asBoolean(false)) {
                                    fields.add(field.path("full_name").asText());
                                }
                            }
                        }
                        result.put(indexEntry.getKey(), fields.build());
                    }
                    return result.buildOrThrow();
                }
                catch (IOException e) {
                    throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
                }
            });
        }
        catch (TrinoException e) {
            if (e.getCause() instanceof ResponseException responseException && responseException.getResponse().getStatusLine().getStatusCode() == 404) {
                // Closed indices retain mappings but cannot supply field defaults.
                return ImmutableMap.of();
            }
            throw e;
        }
    }

    private IndexMetadata.ObjectType parseIndexMetadata(JsonNode mappings, Optional<Set<String>> ignoredMalformedFields)
    {
        if (!mappings.elements().hasNext()) {
            return new IndexMetadata.ObjectType(ImmutableList.of());
        }
        if (!mappings.has("properties")) {
            // Older versions of ElasticSearch supported multiple "type" mappings
            // for a given index. Newer versions support only one and don't
            // expose it in the document. Here we skip it if it's present.
            mappings = mappings.elements().next();
            if (!mappings.has("properties")) {
                return new IndexMetadata.ObjectType(ImmutableList.of());
            }
        }

        JsonNode metaNode = nullSafeNode(mappings, "_meta");
        JsonNode metaProperties = nullSafeNode(metaNode, "trino");

        // stay backwards compatible with _meta.presto namespace for some releases
        if (metaProperties.isNull()) {
            metaProperties = nullSafeNode(metaNode, "presto");
        }

        JsonNode properties = mappings.get("properties");
        boolean unsupportedAncestor = !mappings.path("enabled").asBoolean(true)
                || !mappings.path("_source").path("enabled").asBoolean(true)
                || ignoredMalformedFields.isEmpty();
        ImmutableSet.Builder<String> unsupportedFields = ImmutableSet.builder();
        unsupportedFields.addAll(ignoredMalformedFields.orElse(ImmutableSet.of()));
        unsupportedFields.addAll(mappings.path("runtime").fieldNames());
        collectCopyToTargets(properties, unsupportedFields);
        MappingSafety mappingSafety = new MappingSafety(
                unsupportedFields.build(),
                sourceFilterPatterns(mappings.path("_source").path("includes")),
                sourceFilterPatterns(mappings.path("_source").path("excludes")));
        return parseType(properties, metaProperties, unsupportedAncestor, "", mappingSafety);
    }

    private static IndexMetadata.ObjectType mergeMappingSafety(IndexMetadata.ObjectType schema, List<IndexMetadata.ObjectType> otherSchemas)
    {
        List<ImmutableMap<String, IndexMetadata.Field>> otherFieldsByName = otherSchemas.stream()
                .map(otherSchema -> otherSchema.fields().stream()
                        .collect(toImmutableMap(IndexMetadata.Field::name, Function.identity(), (first, _) -> first)))
                .toList();
        ImmutableList.Builder<IndexMetadata.Field> fields = ImmutableList.builder();
        for (IndexMetadata.Field field : schema.fields()) {
            boolean supportsPredicates = field.supportsPredicates();
            boolean supportsTopNAndAggregations = field.supportsTopNAndAggregations();
            List<IndexMetadata.Field> matchingFields = new ArrayList<>();
            for (Map<String, IndexMetadata.Field> otherFields : otherFieldsByName) {
                IndexMetadata.Field matchingField = otherFields.get(field.name());
                if (matchingField == null) {
                    supportsPredicates = false;
                    supportsTopNAndAggregations = false;
                    continue;
                }
                matchingFields.add(matchingField);
                boolean compatibleRepresentation = field.asRawJson() == matchingField.asRawJson()
                        && field.isArray() == matchingField.isArray();
                supportsPredicates &= compatibleRepresentation && matchingField.supportsPredicates();
                supportsTopNAndAggregations &= compatibleRepresentation && matchingField.supportsTopNAndAggregations();
            }

            IndexMetadata.Type type = field.type();
            if (field.type() instanceof IndexMetadata.ObjectType objectType) {
                List<IndexMetadata.ObjectType> matchingObjectTypes = matchingFields.stream()
                        .map(IndexMetadata.Field::type)
                        .filter(IndexMetadata.ObjectType.class::isInstance)
                        .map(IndexMetadata.ObjectType.class::cast)
                        .toList();
                if (matchingObjectTypes.size() != otherSchemas.size()) {
                    supportsPredicates = false;
                    supportsTopNAndAggregations = false;
                }
                else {
                    type = mergeMappingSafety(objectType, matchingObjectTypes);
                }
                if (!supportsPredicates) {
                    type = disablePredicatePushdown((IndexMetadata.ObjectType) type);
                }
            }
            else {
                boolean compatibleType = matchingFields.size() == otherSchemas.size()
                        && matchingFields.stream().allMatch(matchingField -> matchingField.type().equals(field.type()));
                supportsPredicates &= compatibleType;
                supportsTopNAndAggregations &= compatibleType;
            }
            fields.add(new IndexMetadata.Field(field.asRawJson(), field.isArray(), field.name(), type, supportsPredicates, supportsTopNAndAggregations));
        }
        return new IndexMetadata.ObjectType(fields.build());
    }

    private static IndexMetadata.ObjectType disablePredicatePushdown(IndexMetadata.ObjectType type)
    {
        return new IndexMetadata.ObjectType(type.fields().stream()
                .map(field -> new IndexMetadata.Field(
                        field.asRawJson(),
                        field.isArray(),
                        field.name(),
                        field.type() instanceof IndexMetadata.ObjectType objectType ? disablePredicatePushdown(objectType) : field.type(),
                        false,
                        field.supportsTopNAndAggregations()))
                .toList());
    }

    private IndexMetadata.ObjectType parseType(JsonNode properties, JsonNode metaProperties, boolean unsupportedAncestor, String parentPath, MappingSafety mappingSafety)
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
            String path = parentPath.isEmpty() ? name : parentPath + "." + name;
            JsonNode metaNode = nullSafeNode(metaProperties, name);
            boolean isArray = !metaNode.isNull() && metaNode.has("isArray") && metaNode.get("isArray").asBoolean();
            boolean asRawJson = !metaNode.isNull() && metaNode.has("asRawJson") && metaNode.get("asRawJson").asBoolean();
            boolean supportsPredicates = !unsupportedAncestor && !asRawJson && !isArray
                    && mappingSafety.supportsPushdown(path, type.equals("object") || type.equals("nested"))
                    && mappingSupportsPredicates(type, value);
            boolean supportsTopNAndAggregations = supportsPredicates && value.path("doc_values").asBoolean(true);

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
                    result.add(new IndexMetadata.Field(asRawJson, isArray, name, new IndexMetadata.DateTimeType(formats), supportsPredicates, supportsTopNAndAggregations));
                }
                case "scaled_float" -> result.add(new IndexMetadata.Field(asRawJson, isArray, name, new IndexMetadata.ScaledFloatType(value.get("scaling_factor").asDouble()), supportsPredicates, supportsTopNAndAggregations));
                case "nested", "object" -> {
                    if (value.has("properties")) {
                        boolean unsupportedChild = unsupportedAncestor || type.equals("nested") || !value.path("enabled").asBoolean(true) || mappingSafety.unsupportedFields().contains(path);
                        result.add(new IndexMetadata.Field(asRawJson, isArray, name, parseType(value.get("properties"), metaNode, unsupportedChild, path, mappingSafety), supportsPredicates, supportsTopNAndAggregations));
                    }
                    else {
                        LOG.debug("Ignoring empty object field: %s", name);
                    }
                }
                default -> result.add(new IndexMetadata.Field(asRawJson, isArray, name, new IndexMetadata.PrimitiveType(type), supportsPredicates, supportsTopNAndAggregations));
            }
        }

        return new IndexMetadata.ObjectType(result.build());
    }

    private static boolean mappingSupportsPredicates(String type, JsonNode mapping)
    {
        if (mapping.has("script")) {
            return false;
        }
        if (!mapping.path("index").asBoolean(true) && !mapping.path("doc_values").asBoolean(true)) {
            return false;
        }
        if (mapping.has("null_value") || mapping.path("ignore_malformed").asBoolean(false)) {
            return false;
        }
        if (!type.equals("keyword")) {
            return true;
        }
        return !mapping.has("ignore_above") && !mapping.has("normalizer");
    }

    private static List<Pattern> sourceFilterPatterns(JsonNode patterns)
    {
        if (patterns.isTextual()) {
            return ImmutableList.of(sourceFilterPattern(patterns.asText()));
        }
        ImmutableList.Builder<Pattern> result = ImmutableList.builder();
        for (JsonNode pattern : patterns) {
            result.add(sourceFilterPattern(pattern.asText()));
        }
        return result.build();
    }

    private static Pattern sourceFilterPattern(String pattern)
    {
        String regex = Arrays.stream(pattern.split("\\*", -1))
                .map(Pattern::quote)
                .collect(joining(".*"));
        // Matching an object in Elasticsearch's source filter also matches all of its descendants.
        return Pattern.compile(regex + "(?:\\..*)?", Pattern.DOTALL);
    }

    private record MappingSafety(Set<String> unsupportedFields, List<Pattern> sourceIncludes, List<Pattern> sourceExcludes)
    {
        public boolean supportsPushdown(String path, boolean objectField)
        {
            return !unsupportedFields.contains(path)
                    && (objectField || ((sourceIncludes.isEmpty() || sourceIncludes.stream().anyMatch(pattern -> pattern.matcher(path).matches()))
                    && sourceExcludes.stream().noneMatch(pattern -> pattern.matcher(path).matches())));
        }
    }

    private static void collectCopyToTargets(JsonNode properties, ImmutableSet.Builder<String> targets)
    {
        for (JsonNode mapping : properties) {
            JsonNode copyTo = mapping.path("copy_to");
            if (copyTo.isTextual()) {
                targets.add(copyTo.asText());
            }
            else {
                copyTo.forEach(target -> targets.add(target.asText()));
            }
            collectCopyToTargets(mapping.path("properties"), targets);
            collectCopyToTargets(mapping.path("fields"), targets);
        }
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

    public SearchResult beginSearch(String index, int shard, JsonNode query, Optional<List<String>> fields, List<String> documentFields, List<TopNSortItem> sortItems, OptionalLong limit)
    {
        ObjectNode searchBody = JSON.objectNode();
        searchBody.set("query", query);

        int size;
        if (limit.isPresent() && limit.orElseThrow() < scrollSize) {
            size = toIntExact(limit.orElseThrow());
        }
        else {
            size = scrollSize;
        }
        searchBody.put("size", size);

        if (!sortItems.isEmpty()) {
            ArrayNode sort = JSON.arrayNode();
            sortItems.forEach(item -> sort.add(item.toSortQuery()));
            searchBody.set("sort", sort);
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

        String endpoint = format("/%s/_search?preference=_shards:%s&scroll=%sms&pre_filter_shard_size=1&allow_partial_search_results=false", index, shard, scrollTimeout.toMillis());

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

    public AggregationSearchResult beginAggregationSearch(String index, JsonNode query, JsonNode aggregations)
    {
        ObjectNode searchBody = JSON.objectNode();
        searchBody.set("query", query);
        searchBody.set("aggregations", aggregations);
        searchBody.put("size", 0);
        searchBody.put("track_total_hits", true);

        LOG.debug("Begin aggregation search: %s, query: %s", index, searchBody);

        long start = System.nanoTime();
        try {
            Response response = client.performRequest(
                    "POST",
                    format("/%s/_search?allow_partial_search_results=false", index),
                    ImmutableMap.of(),
                    new StringEntity(searchBody.toString(), UTF_8),
                    new BasicHeader("Content-Type", "application/json"));
            try {
                return parseAggregationResponse(EntityUtils.toByteArray(response.getEntity()));
            }
            catch (IOException e) {
                throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
            }
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

    @VisibleForTesting
    static AggregationSearchResult parseAggregationResponse(byte[] responseBody)
            throws IOException
    {
        JsonNode response = parseCompleteSearchResponse(responseBody, "aggregation");
        return new AggregationSearchResult(response, extractExactHitCount(response), responseBody.length);
    }

    private static JsonNode parseCompleteSearchResponse(byte[] responseBody, String operation)
            throws IOException
    {
        JsonNode response = JSON_MAPPER.readTree(responseBody);
        if (response == null || !response.path("timed_out").isBoolean() || !response.path("_shards").path("failed").isIntegralNumber()) {
            throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, format("Elasticsearch %s response is missing search completion information", operation));
        }
        if (response.path("timed_out").asBoolean()) {
            throw new TrinoException(ELASTICSEARCH_QUERY_FAILURE, format("Elasticsearch %s search timed out", operation));
        }
        if (response.path("_shards").path("failed").asInt() > 0) {
            throw new TrinoException(ELASTICSEARCH_QUERY_FAILURE, format("Elasticsearch %s search failed on one or more shards: %s", operation, response.path("_shards").path("failures")));
        }
        return response;
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
        countBody.put("size", 0);
        countBody.put("track_total_hits", true);

        LOG.debug("Count: %s:%s, query: %s", index, shard, countBody);

        long start = System.nanoTime();
        try {
            Response response;
            try {
                response = client.performRequest(
                        "POST",
                        format("/%s/_search?preference=_shards:%s&pre_filter_shard_size=1&allow_partial_search_results=false", index, shard),
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
                return parseCountResponse(EntityUtils.toByteArray(response.getEntity()));
            }
            catch (IOException e) {
                throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
            }
        }
        finally {
            countStats.add(Duration.nanosSince(start));
        }
    }

    @VisibleForTesting
    static long parseCountResponse(byte[] responseBody)
            throws IOException
    {
        return extractExactHitCount(parseCompleteSearchResponse(responseBody, "count"));
    }

    private static long extractExactHitCount(JsonNode response)
    {
        JsonNode total = response.path("hits").path("total");
        JsonNode value = total.path("value");
        if (!"eq".equals(total.path("relation").asText()) || !value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
            throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, "Elasticsearch count response is missing an exact nonnegative hit count");
        }
        return value.longValue();
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

    private static SearchResult parseSearchResponse(Response response)
    {
        try {
            return parseSearchResponse(EntityUtils.toByteArray(response.getEntity()));
        }
        catch (IOException e) {
            throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, e);
        }
    }

    @VisibleForTesting
    static SearchResult parseSearchResponse(byte[] responseBody)
            throws IOException
    {
        JsonNode root = parseCompleteSearchResponse(responseBody, "scan");

        Optional<String> scrollId = Optional.ofNullable(root.get("_scroll_id"))
                .map(JsonNode::asText);

        JsonNode hitsNode = root.path("hits").path("hits");
        if (!hitsNode.isArray()) {
            throw new TrinoException(ELASTICSEARCH_INVALID_RESPONSE, "Elasticsearch scan response is missing search hits");
        }
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
