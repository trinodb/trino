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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.kafka.schema.ContentSchemaProvider;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.Optional;

import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private final KafkaConsumerFactory consumerFactory;
    private final KafkaFilterManager kafkaFilterManager;
    private final ContentSchemaProvider contentSchemaProvider;
    private final int messagesPerSplit;
    private final TopicPartitionOffsetProvider topicPartitionOffsetProvider;

    @Inject
    public KafkaSplitManager(KafkaConsumerFactory consumerFactory, KafkaConfig kafkaConfig, KafkaFilterManager kafkaFilterManager, ContentSchemaProvider contentSchemaProvider, TopicPartitionOffsetProvider topicPartitionOffsetProvider)
    {
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory is null");
        this.messagesPerSplit = kafkaConfig.getMessagesPerSplit();
        this.kafkaFilterManager = requireNonNull(kafkaFilterManager, "kafkaFilterManager is null");
        this.contentSchemaProvider = requireNonNull(contentSchemaProvider, "contentSchemaProvider is null");
        this.topicPartitionOffsetProvider = requireNonNull(topicPartitionOffsetProvider, "topicPartitionOffsetProvider is null");
    }

    static TopicPartition toTopicPartition(PartitionInfo partitionInfo)
    {
        return new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        KafkaTableHandle kafkaTableHandle = (KafkaTableHandle) table;
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = consumerFactory.create(session)) {
            KafkaPartitionInputs initialInputs = topicPartitionOffsetProvider.offsetRangesPerPartition(kafkaConsumer, kafkaTableHandle.topicName(), session);
            KafkaPartitionInputs kafkaPartitionInputs = kafkaFilterManager.getKafkaFilterResult(session, kafkaTableHandle, initialInputs);

            ImmutableList.Builder<KafkaSplit> splits = ImmutableList.builder();
            Optional<String> keyDataSchemaContents = contentSchemaProvider.getKey(kafkaTableHandle);
            Optional<String> messageDataSchemaContents = contentSchemaProvider.getMessage(kafkaTableHandle);

            for (PartitionInfo partitionInfo : kafkaPartitionInputs.partitionInfos()) {
                TopicPartition topicPartition = toTopicPartition(partitionInfo);
                HostAddress leader = HostAddress.fromParts(partitionInfo.leader().host(), partitionInfo.leader().port());
                new Range(kafkaPartitionInputs.partitionBeginOffsets().get(topicPartition), kafkaPartitionInputs.partitionEndOffsets().get(topicPartition))
                        .partition(messagesPerSplit).stream()
                        .map(range -> new KafkaSplit(
                                kafkaTableHandle.topicName(),
                                kafkaTableHandle.keyDataFormat(),
                                kafkaTableHandle.messageDataFormat(),
                                keyDataSchemaContents,
                                messageDataSchemaContents,
                                partitionInfo.partition(),
                                range,
                                leader))
                        .forEach(splits::add);
            }
            return new FixedSplitSource(splits.build());
        }
        catch (Exception e) { // Catch all exceptions because Kafka library is written in scala and checked exceptions are not declared in method signature.
            if (e instanceof TrinoException) {
                throw e;
            }
            throw new TrinoException(KAFKA_SPLIT_ERROR, format("Cannot list splits for table '%s' reading topic '%s'", kafkaTableHandle.tableName(), kafkaTableHandle.topicName()), e);
        }
    }
}
