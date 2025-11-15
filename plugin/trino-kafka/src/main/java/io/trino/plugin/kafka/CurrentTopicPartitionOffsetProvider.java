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

import io.trino.spi.connector.ConnectorSession;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class CurrentTopicPartitionOffsetProvider
        implements TopicPartitionOffsetProvider
{
    public CurrentTopicPartitionOffsetProvider()
    {
    }

    @Override
    public KafkaPartitionInputs offsetRangesPerPartition(KafkaConsumer<byte[], byte[]> kafkaConsumer, String topicName, ConnectorSession session)
    {
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topicName);

        List<TopicPartition> topicPartitions = partitionInfos.stream()
                .map(KafkaSplitManager::toTopicPartition)
                .collect(toImmutableList());

        Map<TopicPartition, Long> partitionBeginOffsets = kafkaConsumer.beginningOffsets(topicPartitions);
        Map<TopicPartition, Long> partitionEndOffsets = kafkaConsumer.endOffsets(topicPartitions);
        return new KafkaPartitionInputs(partitionInfos, partitionBeginOffsets, partitionEndOffsets);
    }
}
