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

package io.trino.plugin.influxdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.trino.spi.connector.SchemaTableName;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.builder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.influxdb.InfluxConstant.ColumnKind.FIELD;
import static io.trino.plugin.influxdb.InfluxConstant.ColumnKind.TAG;
import static io.trino.plugin.influxdb.InfluxConstant.ColumnKind.TIME;
import static io.trino.plugin.influxdb.InfluxConstant.ColumnName;
import static io.trino.plugin.influxdb.TypeUtils.TIMESTAMP;
import static io.trino.plugin.influxdb.TypeUtils.toTrinoType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static okhttp3.logging.HttpLoggingInterceptor.Level.NONE;
import static org.influxdb.dto.QueryResult.Result;
import static org.influxdb.dto.QueryResult.Series;

public class NativeInfluxClient
        implements InfluxClient
{
    private static final Logger log = Logger.get(NativeInfluxClient.class);

    private static final List<String> INTERNAL_SCHEMAS = ImmutableList.of("_internal");
    private static final String SHOW_DATABASE_CMD = "SHOW DATABASES";
    private static final String SHOW_MEASUREMENTS_CMD = "SHOW MEASUREMENTS";
    private static final String SHOW_TAG_KEYS_CMD = "SHOW TAG KEYS FROM \"$measurement\"";
    private static final String SHOW_FIELD_KEYS_CMD = "SHOW FIELD KEYS FROM \"$measurement\"";
    private final InfluxDB client;

    @Inject
    public NativeInfluxClient(InfluxConfig config)
    {
        OkHttpClient.Builder httpBuilder = new OkHttpClient.Builder()
                .connectionPool(new ConnectionPool(10, 10, MINUTES))
                .connectTimeout(config.getConnectTimeOut().toMillis(), MILLISECONDS)
                .readTimeout(config.getReadTimeOut().toMillis(), MILLISECONDS)
                .addInterceptor(new HttpLoggingInterceptor().setLevel(NONE));

        if (config.getUsername().isEmpty() || config.getPassword().isEmpty()) {
            this.client = InfluxDBFactory.connect(config.getEndpoint(), httpBuilder);
        }
        else {
            this.client = InfluxDBFactory.connect(config.getEndpoint(), config.getUsername().get(), config.getPassword().get(), httpBuilder);
        }
    }

    @Override
    public InfluxRecord query(Query query)
    {
        try {
            QueryResult queryResult = client.query(query);
            requireNonNull(queryResult);

            if (queryResult.getResults() == null || queryResult.getResults().isEmpty()) {
                return new InfluxRecord(ImmutableList.of(), ImmutableList.of());
            }
            Result result = getOnlyElement(queryResult.getResults());
            if (result.getSeries() == null || result.getSeries().isEmpty()) {
                return new InfluxRecord(ImmutableList.of(), ImmutableList.of());
            }

            if (result.getSeries().size() == 1) {
                Series series = getOnlyElement(result.getSeries());
                return new InfluxRecord(series.getColumns(), series.getValues());
            }
            else {
                ImmutableList.Builder<String> columns = builder();
                List<List<Object>> values = new ArrayList<>();
                Series firstSeries = result.getSeries().get(0);
                columns.addAll(firstSeries.getColumns());
                columns.addAll(firstSeries.getTags().keySet());
                for (Series series : result.getSeries()) {
                    List<Object> value = new ArrayList<>();
                    value.addAll(series.getValues().get(0));
                    value.addAll(series.getTags().values());
                    values.add(value);
                }
                return new InfluxRecord(columns.build(), values);
            }
        }
        catch (Throwable e) {
            log.error("InfluxDB query error: %s.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSchemaNames()
    {
        QueryResult result = client.query(new Query(SHOW_DATABASE_CMD));
        requireNonNull(result);

        List<Result> results = result.getResults();
        if (results == null || results.isEmpty()) {
            return ImmutableList.of();
        }
        List<Series> series = getOnlyElement(results).getSeries();
        if (series == null || series.isEmpty()) {
            return ImmutableList.of();
        }
        List<List<Object>> databases = getOnlyElement(series).getValues();
        if (databases == null || databases.isEmpty()) {
            return ImmutableList.of();
        }
        return databases.stream()
                .map(database -> getOnlyElement(database).toString())
                .filter(name -> !INTERNAL_SCHEMAS.contains(name.toLowerCase(ENGLISH)))
                .collect(toImmutableList());
    }

    @Override
    public List<SchemaTableName> getSchemaTableNames(String schemaName)
    {
        QueryResult result = client.query(new Query(SHOW_MEASUREMENTS_CMD, schemaName));
        requireNonNull(result);

        List<Result> results = result.getResults();
        if (results == null || results.isEmpty()) {
            return ImmutableList.of();
        }
        List<Series> series = getOnlyElement(results).getSeries();
        if (series == null || series.isEmpty()) {
            return ImmutableList.of();
        }
        List<List<Object>> measurements = getOnlyElement(series).getValues();
        if (measurements == null || measurements.isEmpty()) {
            return ImmutableList.of();
        }
        return measurements.stream()
                .map(measurement -> getOnlyElement(measurement).toString())
                .distinct()
                .map(measurement -> new SchemaTableName(schemaName, measurement))
                .collect(toImmutableList());
    }

    @Override
    public Optional<InfluxTableHandle> getTableHandle(String schemaName, String tableName)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");

        if (!this.getSchemaTableNames(schemaName).contains(new SchemaTableName(schemaName, tableName))) {
            return Optional.empty();
        }

        String fieldKeysCommand = SHOW_FIELD_KEYS_CMD.replace("$measurement", tableName);
        String tagKeysCommand = SHOW_TAG_KEYS_CMD.replace("$measurement", tableName);
        QueryResult fieldResult = client.query(new Query(fieldKeysCommand, schemaName));
        QueryResult tagResult = client.query(new Query(tagKeysCommand, schemaName));

        List<InfluxColumnHandle> columns = this.buildColumns(fieldResult, tagResult);
        return Optional.of(new InfluxTableHandle(schemaName, tableName, columns));
    }

    private List<InfluxColumnHandle> buildColumns(QueryResult fieldResult, QueryResult tagResult)
    {
        ImmutableList.Builder<InfluxColumnHandle> columnBuilder = ImmutableList.builder();
        columnBuilder.add(new InfluxColumnHandle(ColumnName.TIME.getName(), toTrinoType(TIMESTAMP), TIME));

        if (fieldResult.getResults() != null && !fieldResult.getResults().isEmpty()) {
            ImmutableList<InfluxColumnHandle> columnHandles = buildInfluxFieldColumns(fieldResult.getResults());
            columnBuilder.addAll(columnHandles);
        }
        if (tagResult.getResults() != null && !tagResult.getResults().isEmpty()) {
            ImmutableList<InfluxColumnHandle> columnHandles = buildInfluxTagColumns(tagResult.getResults());
            columnBuilder.addAll(columnHandles);
        }
        return columnBuilder.build();
    }

    private ImmutableList<InfluxColumnHandle> buildInfluxFieldColumns(List<Result> result)
    {
        return buildInfluxColumns(getOnlyElement(result), false, true);
    }

    private ImmutableList<InfluxColumnHandle> buildInfluxTagColumns(List<Result> result)
    {
        return buildInfluxColumns(getOnlyElement(result), true, false);
    }

    private ImmutableList<InfluxColumnHandle> buildInfluxColumns(Result result, boolean isTag, boolean isField)
    {
        ImmutableList.Builder<InfluxColumnHandle> columnBuilder = ImmutableList.builder();

        List<Series> series = result.getSeries();
        if (series == null || series.isEmpty()) {
            return ImmutableList.of();
        }
        List<String> columns = getOnlyElement(series).getColumns();
        List<List<Object>> values = getOnlyElement(series).getValues();
        if (columns == null || columns.isEmpty() || values == null || values.isEmpty()) {
            return ImmutableList.of();
        }

        if (isField) {
            Set<String> fieldSetKeys = Sets.newHashSetWithExpectedSize(values.size());
            for (List<Object> fields : values) {
                String fieldKey = (String) fields.get(0);
                String fieldType = (String) fields.get(1);
                String filedSetKey = fieldKey.toLowerCase(ENGLISH);
                if (!fieldSetKeys.contains(filedSetKey)) {
                    columnBuilder.add(new InfluxColumnHandle(fieldKey, toTrinoType(fieldType), FIELD));
                    fieldSetKeys.add(filedSetKey);
                }
            }
        }
        if (isTag) {
            Set<String> tagSetKeys = Sets.newHashSetWithExpectedSize(values.size());
            for (List<Object> tags : values) {
                for (Object tag : tags) {
                    String tagKey = String.valueOf(tag);
                    String tagSetKey = tagKey.toLowerCase(ENGLISH);
                    if (!tagSetKeys.contains(tagSetKey)) {
                        columnBuilder.add(new InfluxColumnHandle(tagKey, VARCHAR, TAG));
                        tagSetKeys.add(tagSetKey);
                    }
                }
            }
        }

        return columnBuilder.build();
    }
}
