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

package io.prestosql.plugin.influx;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.spi.HostAddress;

import javax.inject.Inject;

import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class InfluxClient
{

    final Logger logger;
    private final InfluxConfig config;
    // the various metadata are cached for a configurable number of milliseconds so we don't hammer the server
    private final CachedMetaData<Map<String, String>> retentionPolicies;
    private final CachedMetaData<Map<String, String>> measurements;
    private final Map<String, CachedMetaData<Map<String, InfluxColumn>>> tagKeys;
    private final Map<String, Map<String, CachedMetaData<Map<String, InfluxColumn>>>> fields;

    @Inject
    public InfluxClient(InfluxConfig config)
    {
        this.logger = Logger.get(getClass());
        this.config = requireNonNull(config, "config is null");
        this.retentionPolicies = new CachedMetaData<>(() -> showNames("SHOW RETENTION POLICIES"));
        this.measurements = new CachedMetaData<>(() -> showNames("SHOW MEASUREMENTS"));
        this.tagKeys = new ConcurrentHashMap<>();
        this.fields = new ConcurrentHashMap<>();
    }

    public Collection<String> getSchemaNames()
    {
        return retentionPolicies.get().keySet();
    }

    public String getRetentionPolicy(String schemaName)
    {
        return retentionPolicies.get().get(schemaName);
    }

    public Collection<String> getTableNames()
    {
        return measurements.get().keySet();
    }

    public String getMeasurement(String tableName)
    {
        return measurements.get().get(tableName);
    }

    private Map<String, InfluxColumn> getTags(String tableName)
    {
        return tagKeys.computeIfAbsent(tableName,
            k -> new CachedMetaData<>(() -> {
                String measurement = measurements.get().get(tableName);
                if (measurement == null) {
                    return Collections.emptyMap();
                }
                String query = new InfluxQL("SHOW TAG KEYS FROM ")
                    .addIdentifier(measurement)
                    .toString();
                ImmutableMap.Builder<String, InfluxColumn> tags = new ImmutableMap.Builder<>();
                for (Map.Entry<String, String> name : showNames(query).entrySet()) {
                    tags.put(name.getKey(), new InfluxColumn(name.getValue(), "string", InfluxColumn.Kind.TAG));
                }
                return tags.build();
            }
            ))
            .get();
    }

    private Map<String, InfluxColumn> getFields(String schemaName, String tableName)
    {
        return fields.computeIfAbsent(schemaName,
            k -> new HashMap<>())
            .computeIfAbsent(tableName,
                k -> new CachedMetaData<>(() -> {
                    String retentionPolicy = retentionPolicies.get().get(schemaName);
                    String measurement = measurements.get().get(tableName);
                    if (retentionPolicy == null || measurement == null) {
                        return Collections.emptyMap();
                    }
                    String query = new InfluxQL("SHOW FIELD KEYS FROM ")
                        .addIdentifier(retentionPolicy).append('.')
                        .addIdentifier(measurement)
                        .toString();
                    Map<String, InfluxColumn> fields = new HashMap<>();
                    for (JsonNode series : execute(query)) {
                        if (series.has("values")) {
                            for (JsonNode row : series.get("values")) {
                                String name = row.get(0).textValue();
                                String influxType = row.get(1).textValue();
                                InfluxColumn collision = fields.put(name.toLowerCase(), new InfluxColumn(name, influxType, InfluxColumn.Kind.FIELD));
                                if (collision != null) {
                                    InfluxError.IDENTIFIER_CASE_SENSITIVITY.fail("identifier " + name + " collides with " + collision.getInfluxName(), query);
                                }
                            }
                        }
                    }
                    return ImmutableMap.copyOf(fields);
                }))
            .get();
    }

    public boolean tableExistsInSchema(String schemaName, String tableName)
    {
        return !getFields(schemaName, tableName).isEmpty();
    }

    public List<InfluxColumn> getColumns(String schemaName, String tableName)
    {
        Collection<InfluxColumn> fields = getFields(schemaName, tableName).values();
        if (fields.isEmpty()) {
            return Collections.emptyList();
        }
        ImmutableList.Builder<InfluxColumn> columns = new ImmutableList.Builder<>();
        columns.add(InfluxColumn.TIME);
        columns.addAll(getTags(tableName).values());
        columns.addAll(fields);
        return columns.build();
    }

    private Map<String, String> showNames(String query)
    {
        Map<String, String> names = new HashMap<>();
        JsonNode series = execute(query);
        InfluxError.GENERAL.check(series.getNodeType().equals(JsonNodeType.ARRAY), "expecting an array, not " + series, query);
        InfluxError.GENERAL.check(series.size() == 1, "expecting one element, not " + series, query);
        series = series.get(0);
        if (series.has("values")) {
            for (JsonNode row : series.get("values")) {
                String name = row.get(0).textValue();
                String collision = names.put(name.toLowerCase(), name);
                if (collision != null) {
                    InfluxError.IDENTIFIER_CASE_SENSITIVITY.fail("identifier " + name + " collides with " + collision, query);
                }
            }
        }
        return ImmutableMap.copyOf(names);
    }

    JsonNode execute(String query)
    {
        logger.debug("executing: " + query);
        final JsonNode response;
        try {
            URL url = new URL("http://" + config.getUserName() + ":" + config.getPassword() + "@" + config.getHost() + ":" + config.getPort() +
                "/query?db=" + config.getDatabase() + "&q=" + URLEncoder.encode(query, StandardCharsets.UTF_8.toString()));
            response = new ObjectMapper().readTree(url);
        }
        catch (Throwable t) {
            InfluxError.EXTERNAL.fail(t);
            return null;
        }
        JsonNode results = response.get("results");
        InfluxError.GENERAL.check(results.size() == 1, "expecting one result", query);
        JsonNode result = results.get(0);
        if (result.has("error")) {
            InfluxError.GENERAL.fail(result.get("error").asText(), query);
        }
        return result.get("series");
    }

    public HostAddress getHostAddress()
    {
        return HostAddress.fromParts(config.getHost(), config.getPort());
    }

    private class CachedMetaData<T>
    {

        private final Supplier<T> loader;
        private T value;
        private long lastLoaded;

        private CachedMetaData(Supplier<T> loader)
        {
            this.loader = loader;
        }

        public synchronized T get()
        {
            if (System.currentTimeMillis() > lastLoaded + config.getCacheMetaDataMillis()) {
                value = loader.get();
                lastLoaded = System.currentTimeMillis();
            }
            return value;
        }
    }
}
