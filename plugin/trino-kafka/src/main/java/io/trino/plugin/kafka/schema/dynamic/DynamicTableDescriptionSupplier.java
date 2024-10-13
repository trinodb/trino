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
package io.trino.plugin.kafka.schema.dynamic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.log.Logger;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.plugin.kafka.KafkaConfig;
import io.trino.plugin.kafka.KafkaConsumerFactory;
import io.trino.plugin.kafka.KafkaTopicDescription;
import io.trino.plugin.kafka.KafkaTopicFieldGroup;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DynamicTableDescriptionSupplier
        implements TableDescriptionSupplier
{
    public static final String NAME = "dynamic";

    private static final Logger log = Logger.get(DynamicTableDescriptionSupplier.class);

    private final String defaultSchema;
    private final Pattern tableNameRegex;
    private final KafkaConsumerFactory kafkaConsumerFactory;
    private Map<SchemaTableName, KafkaTopicDescription> tableNameCache;
    private long tableNameCacheExpireTs;
    private long tableNameCacheTTLMillis;
    private long tableNameCacheHitCount;

    public DynamicTableDescriptionSupplier(DynamicTableDescriptionSupplierConfig config, KafkaConfig kafkaConfig, KafkaConsumerFactory consumerFactory)
    {
        this.defaultSchema = kafkaConfig.getDefaultSchema();
        this.tableNameRegex = config.getTableNameRegex();
        this.kafkaConsumerFactory = consumerFactory;
        this.tableNameCache = null;
        this.tableNameCacheExpireTs = 0;
        this.tableNameCacheTTLMillis = TimeUnit.SECONDS.toMillis(config.getTableNameCacheTTLSecs());
        this.tableNameCacheHitCount = 0;
    }

    public static class Factory
                implements Provider<TableDescriptionSupplier>
    {
        private final DynamicTableDescriptionSupplierConfig config;
        private final KafkaConfig kafkaConfig;
        private final KafkaConsumerFactory consumerFactory;

        @Inject
        public Factory(
                DynamicTableDescriptionSupplierConfig config,
                KafkaConfig kafkaConfig,
                KafkaConsumerFactory consumerFactory)
        {
            this.config = config;
            this.kafkaConfig = kafkaConfig;
            this.consumerFactory = consumerFactory;
        }

        @Override
        public TableDescriptionSupplier get()
        {
            return new DynamicTableDescriptionSupplier(config, kafkaConfig, consumerFactory);
        }
    }

    @Override
    public Set<SchemaTableName> listTables()
    {
        return getTablesFromKafkaRegex().keySet();
    }

    @Override
    public Optional<KafkaTopicDescription> getTopicDescription(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return Optional.ofNullable(getTablesFromKafkaRegex().get(schemaTableName));
    }

    private Map<String, List<PartitionInfo>> listTopics()
    {
        KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaConsumerFactory.create(null);
        try {
            return kafkaConsumer.listTopics();
        }
        finally {
            kafkaConsumer.close();
        }
    }

    private synchronized Map<SchemaTableName, KafkaTopicDescription> getTablesFromKafkaRegex()
    {
        long currentTs = System.currentTimeMillis();
        if (currentTs > tableNameCacheExpireTs || tableNameCache == null) {
            ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> builder = ImmutableMap.builder();
            Map<String, List<PartitionInfo>> topics = listTopics();
            for (String topicName : topics.keySet()) {
                Matcher m = tableNameRegex.matcher(topicName);
                if (m.matches()) {
                    log.debug("Adding Kafka Topic:'%s', it matches regex '%s'", topicName, tableNameRegex);
                    SchemaTableName tableName = new SchemaTableName(defaultSchema, topicName);
                    builder.put(
                            tableName,
                            new KafkaTopicDescription(
                                topicName,
                                Optional.ofNullable(defaultSchema),
                                topicName,
                                Optional.of(new KafkaTopicFieldGroup(DummyRowDecoder.NAME, Optional.empty(), Optional.empty(), ImmutableList.of())),
                                Optional.of(new KafkaTopicFieldGroup(DummyRowDecoder.NAME, Optional.empty(), Optional.empty(), ImmutableList.of()))));
                }
            }
            tableNameCache = builder.buildOrThrow();
            tableNameCacheExpireTs = currentTs + tableNameCacheTTLMillis;
            log.info("Refreshing cache, hits:%d entries:%d", tableNameCacheHitCount, tableNameCache.size());
            tableNameCacheHitCount = 0;
        }
        else {
            tableNameCacheHitCount += 1;
        }
        return tableNameCache;
    }
}
