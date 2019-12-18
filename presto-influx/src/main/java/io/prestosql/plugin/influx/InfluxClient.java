package io.prestosql.plugin.influx;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.HostAddress;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class InfluxClient {

    private final InfluxConfig config;
    private final InfluxDB influxDB;
    // the various metadata are cached for a configurable number of milliseconds so we don't hammer the server
    private final CachedMetaData<Map<String, String>> retentionPolicies;
    private final CachedMetaData<Map<String, String>> measurements;
    private final Map<String, CachedMetaData<Map<String, InfluxColumn>>> tagKeys;
    private final Map<String, CachedMetaData<Map<String, InfluxColumn>>> fields;

    @Inject
    public InfluxClient(InfluxConfig config) {
        this.config = requireNonNull(config, "config is null");
        this.influxDB = InfluxDBFactory.connect("http://" + config.getHost() + ":" + config.getPort(),
            config.getUserName(), config.getPassword());
        this.retentionPolicies = new CachedMetaData<>(() -> showNames("SHOW RETENTION POLICIES"));
        this.measurements = new CachedMetaData<>(() -> showNames("SHOW MEASUREMENTS"));
        this.tagKeys = new ConcurrentHashMap<>();
        this.fields = new ConcurrentHashMap<>();
    }

    public Map<String, String> getRetentionPolicies() {
        return retentionPolicies.get();
    }

    public Map<String, String> getMeasurements() {
        return measurements.get();
    }

    public Map<String, InfluxColumn> getTags(String measurement) {
        return tagKeys.computeIfAbsent(measurement,
            k -> new CachedMetaData<>(() -> {
                String query = "SHOW TAG KEYS FROM " + getMeasurements().get(measurement);
                ImmutableMap.Builder<String, InfluxColumn> tags = new ImmutableMap.Builder<>();
                for (Map.Entry<String, String> name: showNames(query).entrySet()) {
                    tags.put(name.getKey(), new InfluxColumn(name.getValue(), "string", InfluxColumn.Kind.TAG));
                }
                return tags.build();
            }
            ))
            .get();
    }

    public Map<String, InfluxColumn> getFields(String measurement) {
        return fields.computeIfAbsent(measurement,
            k -> new CachedMetaData<>(() -> {
                String query = "SHOW FIELD KEYS FROM " + getMeasurements().get(measurement);
                Map<String, InfluxColumn> fields = new HashMap<>();
                for (QueryResult.Series series : execute(query)) {
                    int nameIndex = series.getColumns().indexOf("fieldKey");
                    int typeIndex = series.getColumns().indexOf("fieldType");
                    for (List<Object> row : series.getValues()) {
                        String name = row.get(nameIndex).toString();
                        String influxType = row.get(typeIndex).toString();
                        InfluxColumn collision = fields.put(name.toLowerCase(), new InfluxColumn(name, influxType, InfluxColumn.Kind.FIELD));
                        if (collision != null) {
                            InfluxErrorCode.IDENTIFIER_CASE_SENSITIVITY.fail("identifier " + name + " collides with " + collision.getInfluxName(), query);
                        }
                    }
                }
                return ImmutableMap.copyOf(fields);
            }))
            .get();
    }

    public List<InfluxColumn> getColumns(String measurement) {
        ImmutableList.Builder<InfluxColumn> columns = new ImmutableList.Builder<>();
        columns.add(InfluxColumn.TIME);
        columns.addAll(getTags(measurement).values());
        columns.addAll(getFields(measurement).values());
        return columns.build();
    }

    private Map<String, String> showNames(String query) {
        Map<String, String> names = new HashMap<>();
        for (QueryResult.Series series: execute(query)) {
            for (List<Object> row: series.getValues()) {
                String name = row.get(0).toString();
                String collision = names.put(name.toLowerCase(), name);
                if (collision != null) {
                    InfluxErrorCode.IDENTIFIER_CASE_SENSITIVITY.fail("identifier " + name + " collides with " + collision, query);
                }
            }
        }
        return ImmutableMap.copyOf(names);
    }

    List<QueryResult.Series> execute(String query) {
        QueryResult result = influxDB.query(new Query(query, config.getDatabase()));
        InfluxErrorCode.GENERAL_ERROR.check(!result.hasError(), result.getError(), query);
        InfluxErrorCode.GENERAL_ERROR.check(result.getResults().size() == 1, "expecting 1 series", query);
        return result.getResults().get(0).getSeries();
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
