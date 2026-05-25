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
package io.trino.plugin.kafka.schema.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.kafka.KafkaConfig;
import io.trino.plugin.kafka.KafkaTopicDescription;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

/**
 * Fetches table and schema descriptions from a REST API instead of the file system.
 * The API must return a JSON array of KafkaTopicDescription objects (same structure as file-based JSON).
 * Only the configured API URL is called; no external or user-supplied URLs are used.
 */
public class RestTableDescriptionSupplier
        implements TableDescriptionSupplier
{
    public static final String NAME = "rest";

    private static final Logger log = Logger.get(RestTableDescriptionSupplier.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final URI tableDescriptionApiUrl;
    private final String defaultSchema;
    private final JsonCodec<KafkaTopicDescription> topicDescriptionCodec;
    private final long refreshIntervalMillis;
    private final Optional<RestTableDescriptionEnricher> enricher;

    private final AtomicReference<CachedDescriptions> cache = new AtomicReference<>();
    private final ReentrantLock refreshLock = new ReentrantLock();
    private final HttpClient httpClient;

    @Inject
    public RestTableDescriptionSupplier(
            RestTableDescriptionSupplierConfig config,
            KafkaConfig kafkaConfig,
            JsonCodec<KafkaTopicDescription> topicDescriptionCodec,
            Optional<RestTableDescriptionEnricher> enricher)
    {
        this.tableDescriptionApiUrl = requireNonNull(config.getTableDescriptionApiUrl(), "tableDescriptionApiUrl is null");
        this.defaultSchema = requireNonNull(kafkaConfig.getDefaultSchema(), "defaultSchema is null");
        this.topicDescriptionCodec = requireNonNull(topicDescriptionCodec, "topicDescriptionCodec is null");
        this.enricher = requireNonNull(enricher, "enricher is null");
        Duration refresh = config.getRefreshInterval();
        this.refreshIntervalMillis = refresh == null ? 0 : refresh.toMillis();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofSeconds(10))
                .build();
    }

    @Override
    public Set<SchemaTableName> listTables()
    {
        return loadDescriptions().keySet();
    }

    @Override
    public Optional<KafkaTopicDescription> getTopicDescription(ConnectorSession session, SchemaTableName schemaTableName)
    {
        KafkaTopicDescription description = loadDescriptions().get(schemaTableName);
        if (description == null) {
            return Optional.empty();
        }
        if (enricher.isPresent()) {
            description = enricher.get().enrich(session, description);
        }
        return Optional.of(description);
    }

    private Map<SchemaTableName, KafkaTopicDescription> loadDescriptions()
    {
        long now = System.currentTimeMillis();
        CachedDescriptions cached = cache.get();
        if (cached != null && refreshIntervalMillis > 0 && (now - cached.loadedAtMillis) < refreshIntervalMillis) {
            return cached.descriptions;
        }

        if (refreshIntervalMillis > 0) {
            refreshLock.lock();
            try {
                cached = cache.get();
                if (cached != null && (now - cached.loadedAtMillis) < refreshIntervalMillis) {
                    return cached.descriptions;
                }
                return fetchAndCacheDescriptions();
            }
            finally {
                refreshLock.unlock();
            }
        }
        return fetchAndCacheDescriptions();
    }

    private Map<SchemaTableName, KafkaTopicDescription> fetchAndCacheDescriptions()
    {
        ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> builder = ImmutableMap.builder();

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(tableDescriptionApiUrl)
                    .GET()
                    .timeout(java.time.Duration.ofSeconds(30))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            if (response.statusCode() != 200) {
                throw new IllegalStateException("Table description API returned status " + response.statusCode());
            }

            String body = response.body();
            if (body == null || body.isBlank()) {
                log.debug("Table description API returned empty body");
                return builder.buildOrThrow();
            }

            JsonNode root = JSON_MAPPER.readTree(body);
            if (!root.isArray()) {
                throw new IllegalStateException("Table description API must return a JSON array of table descriptions");
            }

            for (JsonNode node : root) {
                KafkaTopicDescription table = topicDescriptionCodec.fromJson(node.toString());
                String schemaName = table.schemaName().orElse(defaultSchema);
                SchemaTableName schemaTableName = new SchemaTableName(schemaName, table.tableName());
                builder.put(schemaTableName, table);
            }

            ImmutableMap<SchemaTableName, KafkaTopicDescription> descriptions = builder.buildOrThrow();
            cache.set(new CachedDescriptions(descriptions, System.currentTimeMillis()));
            log.debug("Loaded %s table descriptions from REST API", descriptions.size());
            return descriptions;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while fetching table descriptions from API", e);
        }
        catch (Exception e) {
            CachedDescriptions existing = cache.get();
            if (existing != null) {
                log.warn(e, "Failed to refresh table descriptions from API, using cached data");
                return existing.descriptions;
            }
            throw new RuntimeException("Failed to load table descriptions from REST API: " + tableDescriptionApiUrl, e);
        }
    }

    private static final class CachedDescriptions
    {
        final Map<SchemaTableName, KafkaTopicDescription> descriptions;
        final long loadedAtMillis;

        CachedDescriptions(Map<SchemaTableName, KafkaTopicDescription> descriptions, long loadedAtMillis)
        {
            this.descriptions = descriptions;
            this.loadedAtMillis = loadedAtMillis;
        }
    }
}
