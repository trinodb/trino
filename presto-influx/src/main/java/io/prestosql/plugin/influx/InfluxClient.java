package io.prestosql.plugin.influx;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.spi.HostAddress;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class InfluxClient {

    final Logger logger;
    private final InfluxConfig config;
    private final InfluxDB influxDB;
    // the various metadata are cached for a configurable number of milliseconds so we don't hammer the server
    private final CachedMetaData<Map<String, String>> retentionPolicies;
    private final CachedMetaData<Map<String, String>> measurements;
    private final Map<String, CachedMetaData<Map<String, InfluxColumn>>> tagKeys;
    private final Map<String, Map<String, CachedMetaData<Map<String, InfluxColumn>>>> fields;

    @Inject
    public InfluxClient(InfluxConfig config) {
        this.logger = Logger.get(getClass());
        this.config = requireNonNull(config, "config is null");
        this.influxDB = InfluxDBFactory.connect("http://" + config.getHost() + ":" + config.getPort(),
            config.getUserName(), config.getPassword());
        this.retentionPolicies = new CachedMetaData<>(() -> showNames("SHOW RETENTION POLICIES"));
        this.measurements = new CachedMetaData<>(() -> showNames("SHOW MEASUREMENTS"));
        this.tagKeys = new ConcurrentHashMap<>();
        this.fields = new ConcurrentHashMap<>();
    }

    public Collection<String> getSchemaNames() {
        return retentionPolicies.get().keySet();
    }

    public String getRetentionPolicy(String schemaName) {
        return retentionPolicies.get().get(schemaName);
    }

    public Collection<String> getTableNames() {
        return measurements.get().keySet();
    }

    public String getMeasurement(String tableName) {
        return measurements.get().get(tableName);
    }

    private Map<String, InfluxColumn> getTags(String tableName) {
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
                for (Map.Entry<String, String> name: showNames(query).entrySet()) {
                    tags.put(name.getKey(), new InfluxColumn(name.getValue(), "string", InfluxColumn.Kind.TAG));
                }
                return tags.build();
            }
            ))
            .get();
    }

    private Map<String, InfluxColumn> getFields(String schemaName, String tableName) {
        return fields.computeIfAbsent(schemaName,
            k -> new HashMap<>())
            .computeIfAbsent(tableName,
                k -> new CachedMetaData<>(() -> {
                    String retentionPolicy =  retentionPolicies.get().get(schemaName);
                    String measurement = measurements.get().get(tableName);
                    if (retentionPolicy == null || measurement == null) {
                        return Collections.emptyMap();
                    }
                    String query = new InfluxQL("SHOW FIELD KEYS FROM ")
                        .addIdentifier(retentionPolicy).append('.')
                        .addIdentifier(measurement)
                        .toString();
                    Map<String, InfluxColumn> fields = new HashMap<>();
                    for (QueryResult.Series series : execute(query)) {
                        int nameIndex = series.getColumns().indexOf("fieldKey");
                        int typeIndex = series.getColumns().indexOf("fieldType");
                        for (List<Object> row : series.getValues()) {
                            String name = row.get(nameIndex).toString();
                            String influxType = row.get(typeIndex).toString();
                            InfluxColumn collision = fields.put(name.toLowerCase(), new InfluxColumn(name, influxType, InfluxColumn.Kind.FIELD));
                            if (collision != null) {
                                InfluxError.IDENTIFIER_CASE_SENSITIVITY.fail("identifier " + name + " collides with " + collision.getInfluxName(), query);
                            }
                        }
                    }
                    return ImmutableMap.copyOf(fields);
                }))
            .get();
    }

    public boolean tableExistsInSchema(String schemaName, String tableName) {
        return !getFields(schemaName, tableName).isEmpty();
    }

    public List<InfluxColumn> getColumns(String schemaName, String tableName) {
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

    private Map<String, String> showNames(String query) {
        Map<String, String> names = new HashMap<>();
        for (QueryResult.Series series: execute(query)) {
            for (List<Object> row: series.getValues()) {
                String name = row.get(0).toString();
                String collision = names.put(name.toLowerCase(), name);
                if (collision != null) {
                    InfluxError.IDENTIFIER_CASE_SENSITIVITY.fail("identifier " + name + " collides with " + collision, query);
                }
            }
        }
        return ImmutableMap.copyOf(names);
    }

    List<QueryResult.Series> execute(String query) {
        logger.debug("executing: " + query);
        QueryResult result;
        try {
            result = influxDB.query(new Query(query, config.getDatabase()));
        } catch (InfluxDBException e) {
            InfluxError.GENERAL.fail(e.toString(), query);
            return Collections.emptyList();
        }
        InfluxError.GENERAL.check(!result.hasError(), result.getError(), query);
        InfluxError.GENERAL.check(result.getResults().size() == 1, "expecting 1 series", query);
        InfluxError.GENERAL.check(!result.getResults().get(0).hasError(), result.getResults().get(0).getError(), query);
        List<QueryResult.Series> series = result.getResults().get(0).getSeries();
        return series != null? series: Collections.emptyList();
    }

    public HostAddress getHostAddress() {
        return HostAddress.fromParts(config.getHost(), config.getPort());
    }

    private class CachedMetaData<T> {

        private final Supplier<T> loader;
        private T value;
        private long lastLoaded;

        private CachedMetaData(Supplier<T> loader) {
            this.loader = loader;
        }

        public synchronized T get() {
            if (System.currentTimeMillis() > lastLoaded + config.getCacheMetaDataMillis()) {
                value = loader.get();
                lastLoaded = System.currentTimeMillis();
            }
            return value;
        }

    }
}
