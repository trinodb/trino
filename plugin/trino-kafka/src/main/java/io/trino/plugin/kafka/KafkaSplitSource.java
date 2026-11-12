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
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class KafkaSplitSource
        implements ConnectorSplitSource
{
    private final KafkaTableHandle table;
    private final List<PartitionInfo> partitionInfos;
    private final Map<TopicPartition, Long> partitionBeginOffsets;
    private final Map<TopicPartition, Long> partitionEndOffsets;
    private final Optional<String> keyDataSchemaContents;
    private final Optional<String> messageDataSchemaContents;
    private final int messagesPerSplit;

    private int partitionIndex;
    private int currentPartitionId = -1;
    private Optional<HostAddress> currentLeader = Optional.empty();
    private long currentOffset;
    private long currentEndOffset;
    private volatile boolean closed;

    public KafkaSplitSource(
            KafkaTableHandle table,
            List<PartitionInfo> partitionInfos,
            Map<TopicPartition, Long> partitionBeginOffsets,
            Map<TopicPartition, Long> partitionEndOffsets,
            Optional<String> keyDataSchemaContents,
            Optional<String> messageDataSchemaContents,
            int messagesPerSplit)
    {
        this.table = requireNonNull(table, "table is null");
        this.partitionInfos = requireNonNull(partitionInfos, "partitionInfos is null");
        this.partitionBeginOffsets = requireNonNull(partitionBeginOffsets, "partitionBeginOffsets is null");
        this.partitionEndOffsets = requireNonNull(partitionEndOffsets, "partitionEndOffsets is null");
        this.keyDataSchemaContents = requireNonNull(keyDataSchemaContents, "keyDataSchemaContents is null");
        this.messageDataSchemaContents = requireNonNull(messageDataSchemaContents, "messageDataSchemaContents is null");
        checkArgument(messagesPerSplit > 0, "messagesPerSplit must be greater than zero");
        this.messagesPerSplit = messagesPerSplit;
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        if (closed) {
            // close() may race with a batch request; returning an empty batch is allowed by the SPI
            return completedFuture(ImmutableList.of());
        }
        if (maxSize <= 0) {
            return completedFuture(ImmutableList.of());
        }

        List<ConnectorSplit> splits = new ArrayList<>(maxSize);
        while (splits.size() < maxSize) {
            while (currentOffset >= currentEndOffset) {
                if (partitionIndex >= partitionInfos.size()) {
                    break;
                }

                PartitionInfo partitionInfo = partitionInfos.get(partitionIndex);
                partitionIndex++;

                TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                long beginOffset = requireNonNull(
                        partitionBeginOffsets.get(topicPartition),
                        () -> format("Missing beginning offset for partition %s", topicPartition));
                long endOffset = requireNonNull(
                        partitionEndOffsets.get(topicPartition),
                        () -> format("Missing end offset for partition %s", topicPartition));

                currentPartitionId = partitionInfo.partition();
                currentOffset = beginOffset;
                currentEndOffset = endOffset;

                try {
                    currentLeader = Optional.ofNullable(partitionInfo.leader())
                            .map(node -> HostAddress.fromParts(node.host(), node.port()));
                }
                catch (IllegalArgumentException e) {
                    throw new TrinoException(KAFKA_SPLIT_ERROR, format("Cannot list splits for table '%s' reading topic '%s'", table.tableName(), table.topicName()), e);
                }
            }

            if (partitionIndex >= partitionInfos.size() && currentOffset >= currentEndOffset) {
                break;
            }

            long rangeEnd = min(currentOffset + messagesPerSplit, currentEndOffset);
            Range range = new Range(currentOffset, rangeEnd);
            currentOffset = rangeEnd;

            splits.add(new KafkaSplit(
                    table.topicName(),
                    table.keyDataFormat(),
                    table.messageDataFormat(),
                    keyDataSchemaContents,
                    messageDataSchemaContents,
                    currentPartitionId,
                    range,
                    currentLeader));
        }
        return completedFuture(ImmutableList.copyOf(splits));
    }

    @Override
    public boolean isFinished()
    {
        return closed || (partitionIndex >= partitionInfos.size() && currentOffset >= currentEndOffset);
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
