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

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.plugin.kafka.KafkaSessionProperties.KAFKA_TOPIC_PARTITION_OFFSET_OVERRIDES;

public class OverriddenTopicPartitionOffsetProvider
        implements TopicPartitionOffsetProvider
{
    public static final String DELIMITER = "-";
    public static final String PARTITION_TO_OFFSET_DELIMITER = "=";
    private final TopicPartitionOffsetProvider offsetProvider;

    public OverriddenTopicPartitionOffsetProvider(TopicPartitionOffsetProvider offsetProvider)
    {
        this.offsetProvider = offsetProvider;
    }

    private static TopicPartitionAndOffsetsString splitTopicPartitionAndOffsets(String topicPartitionAndOffsetString)
    {
        String[] parts = topicPartitionAndOffsetString.split(PARTITION_TO_OFFSET_DELIMITER);
        if (parts.length != 2) {
            throw new TrinoException(StandardErrorCode.INVALID_SESSION_PROPERTY, "String should be two offsets separated by a '-': " + topicPartitionAndOffsetString);
        }
        return new TopicPartitionAndOffsetsString(parts[0], parts[1]);
    }

    private static TopicPartition parseTopicPartition(String topicPartitionString)
    {
        int lastHyphen = topicPartitionString.lastIndexOf(DELIMITER);
        if (lastHyphen == -1) {
            throw new TrinoException(StandardErrorCode.INVALID_SESSION_PROPERTY, "No hyphen in topic partition string: " + topicPartitionString);
        }
        String topic = topicPartitionString.substring(0, lastHyphen);
        String partitionString = topicPartitionString.substring(lastHyphen + 1);
        int partition;
        try {
            partition = Integer.parseInt(partitionString);
        }
        catch (NumberFormatException exception) {
            throw new TrinoException(StandardErrorCode.INVALID_SESSION_PROPERTY, "Partition number not parseable as an int: " + partitionString, exception);
        }
        return new TopicPartition(topic, partition);
    }

    private static long parseBeginningOffset(String offsetString)
    {
        return parseOffset(offsetString, 0);
    }

    private static long parseEndOffset(String offsetString)
    {
        return parseOffset(offsetString, 1);
    }

    private static long parseOffset(String offsetRangeString, int rangeIndex)
    {
        String[] parts = offsetRangeString.split(DELIMITER);
        if (parts.length != 2) {
            throw new TrinoException(StandardErrorCode.INVALID_SESSION_PROPERTY, "String should be two offsets separated by a '-': " + offsetRangeString);
        }
        String offsetString = parts[rangeIndex];
        long offset;
        try {
            offset = Long.parseLong(offsetString);
        }
        catch (NumberFormatException exception) {
            throw new TrinoException(StandardErrorCode.INVALID_SESSION_PROPERTY, "Offset number not parseable as a long: " + offsetString, exception);
        }
        return offset;
    }

    public static KafkaPartitionInputs parseOverriddenInputs(String inputString)
    {
        List<String> perTopicOffsets = Arrays.stream(inputString.replace(" ", "").split(",")).filter(aEntry -> !aEntry.isEmpty()).toList();
        Map<TopicPartition, Long> overriddenPartitionBeginOffsets = perTopicOffsets.stream()
                .map(OverriddenTopicPartitionOffsetProvider::splitTopicPartitionAndOffsets)
                .collect(Collectors.toMap(aEntry -> parseTopicPartition(aEntry.topicPartition()), aEntry -> parseBeginningOffset(aEntry.offsets())));
        Map<TopicPartition, Long> overriddenPartitionEndOffsets = perTopicOffsets.stream()
                .map(OverriddenTopicPartitionOffsetProvider::splitTopicPartitionAndOffsets)
                .collect(Collectors.toMap(aEntry -> parseTopicPartition(aEntry.topicPartition()), aEntry -> parseEndOffset(aEntry.offsets())));
        return new KafkaPartitionInputs(List.of(), overriddenPartitionBeginOffsets, overriddenPartitionEndOffsets);
    }

    @Override
    public KafkaPartitionInputs offsetRangesPerPartition(KafkaConsumer<byte[], byte[]> kafkaConsumer, String topicName, ConnectorSession session)
    {
        KafkaPartitionInputs currentInputs = offsetProvider.offsetRangesPerPartition(kafkaConsumer, topicName, session);

        String offsetOverrideConfig = session.getProperty(KAFKA_TOPIC_PARTITION_OFFSET_OVERRIDES, String.class);
        KafkaPartitionInputs overriddenOffsets = parseOverriddenInputs(offsetOverrideConfig);

        Map<TopicPartition, Long> mergedBeginningOffsets = new HashMap<>(currentInputs.partitionBeginOffsets());
        mergedBeginningOffsets.replaceAll((key, value) -> overriddenOffsets.partitionBeginOffsets().getOrDefault(key, value));

        Map<TopicPartition, Long> mergedEndingOffsets = new HashMap<>(currentInputs.partitionEndOffsets());
        mergedEndingOffsets.replaceAll((key, value) -> overriddenOffsets.partitionEndOffsets().getOrDefault(key, value));

        return new KafkaPartitionInputs(currentInputs.partitionInfos(), mergedBeginningOffsets, mergedEndingOffsets);
    }

    private record TopicPartitionAndOffsetsString(String topicPartition, String offsets) {}
}
