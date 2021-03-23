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
package io.trino.plugin.pinot.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ConnectorTableMetadata;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.pinot.PinotTableProperties.INDEX_AGGREGATE_METRICS;
import static io.trino.plugin.pinot.PinotTableProperties.INDEX_AUTO_GENERATED_INVERTED_INDEX;
import static io.trino.plugin.pinot.PinotTableProperties.INDEX_BLOOM_FILTER;
import static io.trino.plugin.pinot.PinotTableProperties.INDEX_CREATE_INVERTED_DURING_SEGMENT_GENERATION;
import static io.trino.plugin.pinot.PinotTableProperties.INDEX_ENABLE_DEFAULT_STAR_TREE;
import static io.trino.plugin.pinot.PinotTableProperties.INDEX_INVERTED;
import static io.trino.plugin.pinot.PinotTableProperties.INDEX_NO_DICTIONARY;
import static io.trino.plugin.pinot.PinotTableProperties.INDEX_SORTED;
import static io.trino.plugin.pinot.PinotTableProperties.INDEX_STAR_TREE;
import static io.trino.plugin.pinot.PinotTableProperties.INDEX_TEXT_FIELDS;
import static io.trino.plugin.pinot.PinotTableProperties.OFFLINE_BROKER_TENANT;
import static io.trino.plugin.pinot.PinotTableProperties.OFFLINE_REPLICATION_FACTOR;
import static io.trino.plugin.pinot.PinotTableProperties.OFFLINE_RETENTION_DAYS;
import static io.trino.plugin.pinot.PinotTableProperties.OFFLINE_SERVER_TENANT;
import static io.trino.plugin.pinot.PinotTableProperties.REAL_TIME_BROKER_TENANT;
import static io.trino.plugin.pinot.PinotTableProperties.REAL_TIME_CONSUMER_TYPE;
import static io.trino.plugin.pinot.PinotTableProperties.REAL_TIME_FLUSH_THRESHOLD_DESIRED_SIZE;
import static io.trino.plugin.pinot.PinotTableProperties.REAL_TIME_FLUSH_THRESHOLD_TIME;
import static io.trino.plugin.pinot.PinotTableProperties.REAL_TIME_KAFKA_BROKERS;
import static io.trino.plugin.pinot.PinotTableProperties.REAL_TIME_KAFKA_TOPIC;
import static io.trino.plugin.pinot.PinotTableProperties.REAL_TIME_REPLICAS_PER_PARTITION;
import static io.trino.plugin.pinot.PinotTableProperties.REAL_TIME_RETENTION_DAYS;
import static io.trino.plugin.pinot.PinotTableProperties.REAL_TIME_SCHEMA_REGISTRY_URL;
import static io.trino.plugin.pinot.PinotTableProperties.REAL_TIME_SERVER_TENANT;
import static io.trino.plugin.pinot.PinotTableProperties.TIME_FIELD_PROPERTY;
import static io.trino.plugin.pinot.table.ConsumerType.AVRO;
import static java.lang.String.format;

public class PinotTableConfigGenerator
{
    private PinotTableConfigGenerator() {}

    public static Optional<TableConfig> getRealtimeConfig(Schema schema, ConnectorTableMetadata tableMetadata)
    {
        String kafkaTopic = (String) tableMetadata.getProperties().get(REAL_TIME_KAFKA_TOPIC);
        if (isNullOrEmpty(kafkaTopic)) {
            return Optional.empty();
        }
        List<String> kafkaBrokers = (List<String>) tableMetadata.getProperties().get(REAL_TIME_KAFKA_BROKERS);
        checkState(kafkaBrokers != null && !kafkaBrokers.isEmpty(), "No kafka brokers");

        ConsumerType consumerType = (ConsumerType) tableMetadata.getProperties().get(REAL_TIME_CONSUMER_TYPE);
        String schemaRegistryUrl = (String) tableMetadata.getProperties().get(REAL_TIME_SCHEMA_REGISTRY_URL);
        if (consumerType == AVRO) {
            checkState(!isNullOrEmpty(schemaRegistryUrl), "Schema registry url not found");
        }
        int realTimeRetentionDays = (int) tableMetadata.getProperties().get(REAL_TIME_RETENTION_DAYS);
        int realTimeReplicasPerPartition = (int) tableMetadata.getProperties().get(REAL_TIME_REPLICAS_PER_PARTITION);
        String realTimeFlushThresholdTime = (String) tableMetadata.getProperties().get(REAL_TIME_FLUSH_THRESHOLD_TIME);
        String realTimeFlushThresholdDesiredSize = (String) tableMetadata.getProperties().get(REAL_TIME_FLUSH_THRESHOLD_DESIRED_SIZE);
        String realTimeServerTenant = (String) tableMetadata.getProperties().get(REAL_TIME_SERVER_TENANT);
        String realTimeBrokerTenant = (String) tableMetadata.getProperties().get(REAL_TIME_BROKER_TENANT);

        SegmentsValidationAndRetentionConfig config = new SegmentsValidationAndRetentionConfig();
        config.setTimeColumnName((String) tableMetadata.getProperties().get(TIME_FIELD_PROPERTY));
        config.setRetentionTimeUnit(TimeUnit.DAYS.name());
        config.setRetentionTimeValue(String.valueOf(realTimeRetentionDays));
        config.setSegmentPushType("APPEND");
        config.setSegmentPushFrequency("daily");
        config.setSegmentAssignmentStrategy("BalanceNumSegmentAssignmentStrategy");
        config.setSchemaName(schema.getSchemaName());
        config.setReplicasPerPartition(String.valueOf(realTimeReplicasPerPartition));

        IndexingConfig indexingConfig = getIndexingConfig(getFieldNames(schema), tableMetadata);
        ImmutableMap.Builder<String, String> streamConfigsBuilder = ImmutableMap.builder();
        streamConfigsBuilder.put("streamType", "kafka")
                .put("stream.kafka.consumer.type", "LowLevel")
                .put("stream.kafka.topic.name", kafkaTopic)
                .put("stream.kafka.broker.list", String.join(",", kafkaBrokers))
                .put("realtime.segment.flush.threshold.time", realTimeFlushThresholdTime)
                .put("realtime.segment.flush.threshold.size", "0")
                .put("realtime.segment.flush.desired.size", realTimeFlushThresholdDesiredSize)
                .put("isolation.level", "read_committed")
                .put("stream.kafka.consumer.prop.auto.offset.reset", "smallest")
                .put("stream.kafka.consumer.prop.group.id", format("%s_%s", schema.getSchemaName(), UUID.randomUUID()))
                .put("stream.kafka.consumer.prop.client.id", UUID.randomUUID().toString())
                .put("stream.kafka.consumer.factory.class.name", "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory");

        if (consumerType == AVRO) {
            streamConfigsBuilder.put("stream.kafka.decoder.class.name", "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder");
            streamConfigsBuilder.put("stream.kafka.decoder.prop.schema.registry.rest.url", schemaRegistryUrl);
        }
        else {
            streamConfigsBuilder.put("stream.kafka.decoder.class.name", "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder");
        }

        indexingConfig.setStreamConfigs(streamConfigsBuilder.build());

        TableConfig tableConfig = new TableConfig(
                schema.getSchemaName(),
                "REALTIME",
                config,
                new TenantConfig(realTimeBrokerTenant, realTimeServerTenant, null),
                indexingConfig,
                new TableCustomConfig(null),
                null,
                null,
                null,
                null,
                null,
                getTextSearchFields(getFieldNames(schema), tableMetadata),
                null,
                null,
                null);
        return Optional.of(tableConfig);
    }

    public static TableConfig getOfflineConfig(Schema schema, ConnectorTableMetadata tableMetadata)
    {
        int offlineReplicationFactor = (int) tableMetadata.getProperties().get(OFFLINE_REPLICATION_FACTOR);
        int offlineRetentionDays = (int) tableMetadata.getProperties().get(OFFLINE_RETENTION_DAYS);
        String offlineServerTenant = (String) tableMetadata.getProperties().get(OFFLINE_SERVER_TENANT);
        String offlineBrokerTenant = (String) tableMetadata.getProperties().get(OFFLINE_BROKER_TENANT);

        SegmentsValidationAndRetentionConfig config = new SegmentsValidationAndRetentionConfig();
        config.setTimeColumnName((String) tableMetadata.getProperties().get(TIME_FIELD_PROPERTY));
        config.setRetentionTimeUnit(TimeUnit.DAYS.name());
        config.setRetentionTimeValue(String.valueOf(offlineRetentionDays));
        config.setSegmentPushType("APPEND");
        config.setSegmentPushFrequency("daily");
        config.setSegmentAssignmentStrategy("BalanceNumSegmentAssignmentStrategy");
        config.setSchemaName(schema.getSchemaName());
        config.setReplication(String.valueOf(offlineReplicationFactor));
        IndexingConfig indexingConfig = getIndexingConfig(getFieldNames(schema), tableMetadata);

        return new TableConfig(
                schema.getSchemaName(),
                "OFFLINE",
                config,
                new TenantConfig(offlineBrokerTenant, offlineServerTenant, null),
                indexingConfig,
                new TableCustomConfig(null),
                null,
                null,
                null,
                null,
                null,
                getTextSearchFields(getFieldNames(schema), tableMetadata),
                null,
                null,
                null);
    }

    public static IndexingConfig getIndexingConfig(Set<String> fieldNames, ConnectorTableMetadata tableMetadata)
    {
        IndexingConfig indexingConfig = new IndexingConfig();
        List<String> invertedIndexColumns = (List<String>) tableMetadata.getProperties().get(INDEX_INVERTED);
        if (invertedIndexColumns != null && !invertedIndexColumns.isEmpty()) {
            checkState(fieldNames.containsAll(invertedIndexColumns), "Invalid inverted index columns: '%s'", invertedIndexColumns);
            indexingConfig.setInvertedIndexColumns(invertedIndexColumns);
        }
        List<String> noDictionaryColumns = (List<String>) tableMetadata.getProperties().get(INDEX_NO_DICTIONARY);
        if (noDictionaryColumns != null && !noDictionaryColumns.isEmpty()) {
            checkState(fieldNames.containsAll(noDictionaryColumns), "Invalid noDictionaryColumns index columns: '%s'", noDictionaryColumns);
            indexingConfig.setNoDictionaryColumns(noDictionaryColumns);
        }

        List<String> bloomFilterColumns = (List<String>) tableMetadata.getProperties().get(INDEX_BLOOM_FILTER);
        if (bloomFilterColumns != null && !bloomFilterColumns.isEmpty()) {
            checkState(fieldNames.containsAll(bloomFilterColumns), "Invalid noDictionaryColumns index columns: '%s'", noDictionaryColumns);
            indexingConfig.setBloomFilterColumns(bloomFilterColumns);
        }

        List<StarTreeIndexConfig> starTreeIndexConfigs = (List<StarTreeIndexConfig>) tableMetadata.getProperties().get(INDEX_STAR_TREE);
        if (starTreeIndexConfigs != null && !starTreeIndexConfigs.isEmpty()) {
            for (StarTreeIndexConfig starTreeIndexConfig : starTreeIndexConfigs) {
                checkState(fieldNames.containsAll(starTreeIndexConfig.getDimensionsSplitOrder()), "Invalid dimension split order: '%s'", starTreeIndexConfig.getDimensionsSplitOrder());
                starTreeIndexConfig.getFunctionColumnPairs().stream().forEach(pair -> {
                    checkState(fieldNames.contains(pair.split("__", 2)[1]), "Invalid function column pair: '%s'", pair);
                });
                checkState(ImmutableSet.copyOf(starTreeIndexConfig.getDimensionsSplitOrder()).containsAll(Optional.ofNullable(starTreeIndexConfig.getSkipStarNodeCreationForDimensions()).orElse(ImmutableList.of())), "Invalid skipStarNodeCreationForDimensions, all fields must be in dimensionSplitOrder: '%s'", starTreeIndexConfig.getSkipStarNodeCreationForDimensions());
            }
            indexingConfig.setStarTreeIndexConfigs(starTreeIndexConfigs);
        }
        if ((boolean) tableMetadata.getProperties().get(INDEX_ENABLE_DEFAULT_STAR_TREE)) {
            indexingConfig.isEnableDefaultStarTree();
        }
        indexingConfig.setAggregateMetrics((boolean) tableMetadata.getProperties().get(INDEX_AGGREGATE_METRICS));
        indexingConfig.setCreateInvertedIndexDuringSegmentGeneration((boolean) tableMetadata.getProperties().get(INDEX_CREATE_INVERTED_DURING_SEGMENT_GENERATION));
        indexingConfig.setAutoGeneratedInvertedIndex((boolean) tableMetadata.getProperties().get(INDEX_AUTO_GENERATED_INVERTED_INDEX));
        indexingConfig.setLoadMode("MMAP");
        String sortedColumn = (String) tableMetadata.getProperties().get(INDEX_SORTED);
        if (!isNullOrEmpty(sortedColumn)) {
            checkState(fieldNames.contains(sortedColumn), "Invalid sort column '%s'", sortedColumn);
            indexingConfig.setSortedColumn(ImmutableList.of(sortedColumn));
        }
        return indexingConfig;
    }

    public static List<FieldConfig> getTextSearchFields(Set<String> fields, ConnectorTableMetadata tableMetadata)
    {
        List<String> textSearchColumns = (List<String>) tableMetadata.getProperties().get(INDEX_TEXT_FIELDS);

        if (textSearchColumns != null && !textSearchColumns.isEmpty()) {
            checkState(fields.containsAll(textSearchColumns), "Invalid text search columns: '%s'", textSearchColumns);
            ImmutableList.Builder<FieldConfig> fieldConfigBuilder = ImmutableList.builder();
            for (String textSearchColumn : textSearchColumns) {
                fieldConfigBuilder.add(new FieldConfig(
                        textSearchColumn,
                        FieldConfig.EncodingType.RAW,
                        FieldConfig.IndexType.TEXT,
                        ImmutableMap.<String, String>builder()
                                .put("deriveNumDocsPerChunkForRawIndex", "true")
                                .put("rawIndexWriterVersion", "3")
                                .build()));
            }
            return fieldConfigBuilder.build();
        }
        return null;
    }

    private static Set<String> getFieldNames(Schema schema)
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        schema.getDimensionFieldSpecs().stream().map(FieldSpec::getName).forEach(builder::add);
        schema.getMetricFieldSpecs().stream().map(FieldSpec::getName).forEach(builder::add);
        schema.getDateTimeFieldSpecs().stream().map(FieldSpec::getName).forEach(builder::add);
        return builder.build();
    }
}
