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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.pinot.table.ConsumerType;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.utils.JsonUtils;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static io.trino.plugin.pinot.table.ConsumerType.AVRO;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.pinot.spi.data.FieldSpec.FieldType.DIMENSION;

public class PinotTableProperties
{
    // Table Properties
    public static final String SCHEMA_NAME_PROPERTY = "pinot_table_name";
    public static final String TIME_FIELD_PROPERTY = "time_field";

    // Realtime Table Properties
    public static final String REAL_TIME_KAFKA_TOPIC = "kafka_topic";
    public static final String REAL_TIME_KAFKA_BROKERS = "kafka_brokers";
    public static final String REAL_TIME_SCHEMA_REGISTRY_URL = "schema_registry_url";
    public static final String REAL_TIME_RETENTION_DAYS = "realtime_retention";
    public static final String REAL_TIME_REPLICAS_PER_PARTITION = "realtime_replicas_per_partition";
    public static final String REAL_TIME_FLUSH_THRESHOLD_TIME = "realtime_flush_threshold_time";
    public static final String REAL_TIME_FLUSH_THRESHOLD_DESIRED_SIZE = "realtime_flush_threshold_size";
    public static final String REAL_TIME_CONSUMER_TYPE = "realtime_consumer_type";
    public static final String REAL_TIME_SERVER_TENANT = "realtime_server_tenant";
    public static final String REAL_TIME_BROKER_TENANT = "realtime_broker_tenant";

    // Offline Table Properties
    public static final String OFFLINE_REPLICATION_FACTOR = "offline_replication";
    public static final String OFFLINE_RETENTION_DAYS = "offline_retention";
    public static final String OFFLINE_SERVER_TENANT = "offline_server_tenant";
    public static final String OFFLINE_BROKER_TENANT = "offline_broker_tenant";

    // Index properties
    public static final String INDEX_AGGREGATE_METRICS = "index_aggregate_metrics";
    public static final String INDEX_BLOOM_FILTER = "index_bloom_filter";
    public static final String INDEX_INVERTED = "index_inverted";
    public static final String INDEX_CREATE_INVERTED_DURING_SEGMENT_GENERATION = "index_create_during_segment_generation";
    public static final String INDEX_AUTO_GENERATED_INVERTED_INDEX = "index_auto_generated_inverted";
    public static final String INDEX_NO_DICTIONARY = "index_no_dictionary";
    public static final String INDEX_SORTED = "index_sorted";
    public static final String INDEX_STAR_TREE = "index_star_tree";
    public static final String INDEX_ENABLE_DEFAULT_STAR_TREE = "index_enable_default_star_tree";
    public static final String INDEX_TEXT_FIELDS = "index_text_fields";

    // Column Properties
    public static final String PINOT_COLUMN_NAME_PROPERTY = "pinot_column_name";
    public static final String FIELD_TYPE_PROPERTY = "field_type";
    public static final String DEFAULT_VALUE_PROPERTY = "default_value";
    public static final String DATE_TIME_FORMAT_PROPERTY = "format";
    public static final String DATE_TIME_GRANULARITY_PROPERTY = "granularity";
    public static final String DATE_TIME_TRANSFORM_PROPERTY = "transform";

    private final List<PropertyMetadata<?>> tableProperties;
    private final List<PropertyMetadata<?>> columnProperties;

    @Inject
    public PinotTableProperties(PinotConfig config)
    {
        requireNonNull(config, "config is null");
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(PropertyMetadata.stringProperty(
                        SCHEMA_NAME_PROPERTY,
                        "Pinot Schema Name",
                        "",
                        false))
                .add(PropertyMetadata.stringProperty(TIME_FIELD_PROPERTY, "Time field", null, false))
                .add(PropertyMetadata.stringProperty(REAL_TIME_KAFKA_TOPIC, "Kafka topic", "", false))
                .add(new PropertyMetadata<>(
                        REAL_TIME_KAFKA_BROKERS,
                        "Kafka Brokers",
                        new ArrayType(VARCHAR),
                        List.class,
                        null,
                        //config.getDefaultKafkaBrokers(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> (String) name)
                                .collect(Collectors.toList())),
                        value -> value))
                .add(PropertyMetadata.stringProperty(
                        REAL_TIME_SCHEMA_REGISTRY_URL,
                        "Schema Registry Url",
                        //config.getDefaultSchemaRegistryUrl(),
                        null,
                        false))
                .add(PropertyMetadata.integerProperty(REAL_TIME_RETENTION_DAYS, "Real time retention days", 7, false))
                .add(PropertyMetadata.integerProperty(REAL_TIME_REPLICAS_PER_PARTITION, "Real time replicas per partition", 1, false))
                .add(PropertyMetadata.stringProperty(REAL_TIME_FLUSH_THRESHOLD_TIME, "Real time flush threshold time", "6h", false))
                .add(PropertyMetadata.stringProperty(REAL_TIME_FLUSH_THRESHOLD_DESIRED_SIZE, "Real time flush desired size", "200M", false))
                .add(PropertyMetadata.enumProperty(REAL_TIME_CONSUMER_TYPE, "Real time consumer type", ConsumerType.class, AVRO, false))
                .add(PropertyMetadata.stringProperty(REAL_TIME_SERVER_TENANT, "Server tenant for realtime table", "DefaultTenant", false))
                .add(PropertyMetadata.stringProperty(REAL_TIME_BROKER_TENANT, "Broker tenant for realtime table", "DefaultTenant", false))
                .add(PropertyMetadata.integerProperty(OFFLINE_REPLICATION_FACTOR, "Offline replication factor", 1, false))
                .add(PropertyMetadata.integerProperty(OFFLINE_RETENTION_DAYS, "Offline time retention days", 365, false))
                .add(PropertyMetadata.stringProperty(OFFLINE_SERVER_TENANT, "Server tenant for offline table", "DefaultTenant", false))
                .add(PropertyMetadata.stringProperty(OFFLINE_BROKER_TENANT, "Broker tenant for offline table", "DefaultTenant", false))
                .add(new PropertyMetadata<>(
                        INDEX_INVERTED,
                        "Inverted index columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> (String) name)
                                .collect(Collectors.toList())),
                        value -> value))
                .add(new PropertyMetadata<>(
                        INDEX_BLOOM_FILTER,
                        "Bloom filter columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> (String) name)
                                .collect(Collectors.toList())),
                        value -> value))
                .add(new PropertyMetadata<>(
                        INDEX_NO_DICTIONARY,
                        "Inverted index columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> (String) name)
                                .collect(Collectors.toList())),
                        value -> value))
                .add(PropertyMetadata.stringProperty(INDEX_SORTED, "Sorted column", null, false))
                .add(new PropertyMetadata<>(
                        INDEX_STAR_TREE,
                        "Star tree index configs",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(PinotTableProperties::toStarTreeIndexConfig)
                                .collect(Collectors.toList())),
                        value -> value))
                .add(PropertyMetadata.booleanProperty(INDEX_ENABLE_DEFAULT_STAR_TREE, "Enable default star tree", false, false))
                .add(PropertyMetadata.booleanProperty(INDEX_AGGREGATE_METRICS, "Enable pre-aggregation of metrics", true, false))
                .add(PropertyMetadata.booleanProperty(INDEX_CREATE_INVERTED_DURING_SEGMENT_GENERATION, "Create inverted index during segment generation", true, false))
                .add(PropertyMetadata.booleanProperty(INDEX_AUTO_GENERATED_INVERTED_INDEX, "Create auto generated inverted index", false, false))
                .add(new PropertyMetadata<>(
                        INDEX_TEXT_FIELDS,
                        "Text index columns, each column will have a text index",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> (String) name)
                                .collect(Collectors.toList())),
                        value -> value))
                .build();
        columnProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(PropertyMetadata.stringProperty(PINOT_COLUMN_NAME_PROPERTY, "Pinot columnName", null, false))
                .add(PropertyMetadata.enumProperty(
                        FIELD_TYPE_PROPERTY,
                        "Pinot field type: [DIMENSION, METRIC, DATE_TIME]",
                        FieldType.class,
                        DIMENSION,
                        false))
                .add(PropertyMetadata.stringProperty(DEFAULT_VALUE_PROPERTY, "Default value", null, false))
                .add(PropertyMetadata.stringProperty(DATE_TIME_FORMAT_PROPERTY, "Pinot format for date time columns, example: '1:SECONDS:EPOCH'", null, false))
                .add(PropertyMetadata.stringProperty(DATE_TIME_GRANULARITY_PROPERTY, "Pinot granularity for date time columns, example: '1:SECONDS'. Note: This property is required but unused by pinot", null, false))
                .add(PropertyMetadata.stringProperty(DATE_TIME_TRANSFORM_PROPERTY, "Pinot transform for date time columns, example: 'toEpochSeconds(timestamp_ms)'", null, false))
                .build();
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties;
    }

    private static StarTreeIndexConfig toStarTreeIndexConfig(Object value)
    {
        try {
            return JsonUtils.stringToObject((String) value, StarTreeIndexConfig.class);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
