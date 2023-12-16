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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private final KafkaConsumerFactory consumerFactory;
    private final KafkaFilterManager kafkaFilterManager;
    private final ContentSchemaProvider contentSchemaProvider;
    private final int messagesPerSplit;

    @Inject
    public KafkaSplitManager(KafkaConsumerFactory consumerFactory, KafkaConfig kafkaConfig, KafkaFilterManager kafkaFilterManager, ContentSchemaProvider contentSchemaProvider)
    {
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory is null");
        this.messagesPerSplit = kafkaConfig.getMessagesPerSplit();
        this.kafkaFilterManager = requireNonNull(kafkaFilterManager, "kafkaFilterManager is null");
        this.contentSchemaProvider = requireNonNull(contentSchemaProvider, "contentSchemaProvider is null");
    }

    /**
     * Calculates and returns the splits for a given Kafka table based on the specified constraints.
     *
     * @param transaction The transaction handle.
     * @param session The session providing information about the current query.
     * @param table The table handle for which to calculate splits.
     * @param dynamicFilter The dynamic filter applied to the table.
     * @param constraint The constraints applied to the table.
     * @return A ConnectorSplitSource representing the calculated splits.
     *
     * This method creates a Kafka consumer to fetch partition information for the specified topic.
     * It then calculates the starting and ending offsets for each partition and applies filtering based on
     * the given constraints. The method divides each partition into smaller splits, considering the
     * messagesPerSplit setting, and creates a KafkaSplit for each partition segment.
     *
     * The method also handles scenarios where the query includes a limit clause, ensuring that the total number
     * of messages fetched does not exceed the specified limit. It uses range partitioning to divide the data
     * into manageable chunks that can be processed in parallel.
     */
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
            // Fetch partition information for the specified Kafka topic using the consumer.
            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(kafkaTableHandle.getTopicName());

            // Convert PartitionInfo objects to TopicPartition objects.
            List<TopicPartition> topicPartitions = partitionInfos.stream()
                    .map(KafkaSplitManager::toTopicPartition)
                    .collect(toImmutableList());

            // Fetch beginning and end offsets for each topic partition.
            Map<TopicPartition, Long> partitionBeginOffsets = kafkaConsumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> partitionEndOffsets = kafkaConsumer.endOffsets(topicPartitions);
            // Apply filtering based on session and table handle to refine partition information.
            KafkaFilteringResult kafkaFilteringResult = kafkaFilterManager.getKafkaFilterResult(session, kafkaTableHandle,
                    partitionInfos, partitionBeginOffsets, partitionEndOffsets);
            partitionInfos = kafkaFilteringResult.getPartitionInfos();
            partitionBeginOffsets = kafkaFilteringResult.getPartitionBeginOffsets();
            partitionEndOffsets = kafkaFilteringResult.getPartitionEndOffsets();

            int totalRows = 0;

            // Builder to collect the calculated splits.
            ImmutableList.Builder<KafkaSplit> splits = ImmutableList.builder();
            Optional<String> keyDataSchemaContents = contentSchemaProvider.getKey(kafkaTableHandle);
            Optional<String> messageDataSchemaContents = contentSchemaProvider.getMessage(kafkaTableHandle);

            Map<TopicPartition, Long> finalPartitionBeginOffsets = partitionBeginOffsets;
            Map<TopicPartition, Long> finalPartitionEndOffsets = partitionEndOffsets;
            // Iterate over each partition info to create splits.
            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition topicPartition = toTopicPartition(partitionInfo);
                HostAddress leader = HostAddress.fromParts(partitionInfo.leader().host(), partitionInfo.leader().port());

                //Here class to handle the range repartition for each partition.
                class RangeToRepartition
                {
                    private int partitionSize;
                    private OptionalLong limit;

                    {
                        this.partitionSize = messagesPerSplit;
                        this.limit = kafkaTableHandle.getLimit().isPresent() ? kafkaTableHandle.getLimit() : OptionalLong.empty();
                    }

                    public RangeToRepartition() {}

                    public RangeToRepartition(int partitionSize)
                    {
                        this.partitionSize = partitionSize;
                    }

                    private List<Range> getParts()
                    {
                        return new Range(finalPartitionBeginOffsets.get(topicPartition), finalPartitionEndOffsets.get(topicPartition)).partition(partitionSize);
                    }

                    public long size()
                    {
                        return getParts().size();
                    }

                    private Stream<Range> rangeStream()
                    {
                        return getParts().stream();
                    }

                    public Stream<KafkaSplit> stream()
                    {
                        return rangeStream().map(range ->
                           new KafkaSplit(
                                   kafkaTableHandle.getTopicName(),
                                   kafkaTableHandle.getKeyDataFormat(),
                                   kafkaTableHandle.getMessageDataFormat(),
                                   keyDataSchemaContents,
                                   messageDataSchemaContents,
                                   partitionInfo.partition(),
                                   range,
                                   leader,
                                   limit));
                    }

                    public int getPartitionSize()
                    {
                        return partitionSize;
                    }

                    public RangeToRepartition setPartitionSize(int partitionSize)
                    {
                        this.partitionSize = partitionSize;
                        return this;
                    }
                }

                RangeToRepartition repartition = new RangeToRepartition();
                // Check if there's a limit set in the Kafka table handle.
                if (kafkaTableHandle.getLimit().isPresent()) {
                    // Handle the logic for calculating splits when a limit is present.
                    long limit = kafkaTableHandle.getLimit().getAsLong();
                    if (limit > messagesPerSplit && messagesPerSplit > 0) {
                        // It's very true for: round > 0
                        long round = floorDiv(limit, messagesPerSplit);
                        int remainder = (int) (limit % (long) messagesPerSplit);

                        // Check if there are enough partitions to cover the requested limit.
                        // probably: size == 0
                        long size = repartition.size();
                        if (size == 0) {
                            continue;
                        }
                        final Stream<KafkaSplit> splitStream = repartition.stream();

                        // Calculate the number of rows covered by the full rounds.
                        long rowsInRound = splitStream.limit(round)
                                .map(KafkaSplit::getMessagesRange)
                                .mapToLong(r -> r.getEnd() - r.getBegin())
                                .sum();
                        // Calculate the number of rows covered by the remainder.
                        long rowsInReminder = splitStream.skip(round).limit(1L)
                                .map(split ->
                                    split.getSplitByRange(split.getMessagesRange()
                                            .slice(split.getMessagesRange().getBegin(), remainder)))
                                .findFirst()
                                .map(KafkaSplit::getMessagesRange)
                                .map(r -> r.getEnd() - r.getBegin())
                                .orElse(0L);

                        long rows = rowsInRound + rowsInReminder;
                        totalRows += rows;

                        // Check if total rows exceed the limit and adjust accordingly.
                        if (totalRows > limit) {
                            rows -= totalRows - limit;
                            // in fact, the finalRows exists
                            long finalRows = rows;
                            if (finalRows > 0) {
                                // Stream and collect splits within the final rows limit.
                                long round2 = floorDiv(finalRows, messagesPerSplit);
                                int remainder2 = (int) (finalRows % (long) messagesPerSplit);
                                splitStream.limit(round2).forEach(splits::add);
                                // Handle the final partial split if the remainder is non-zero.
                                if (remainder2 > 0) {
                                    splitStream.skip(round2).limit(1L)
                                            .map(split ->
                                                    split.getSplitByRange(split.getMessagesRange()
                                                            .slice(split.getMessagesRange().getBegin(), remainder2)))
                                            .peek(splits::add);
                                }
                            }
                            break;
                        }
                        // we can fetch all the splits if the constraint is size > limit
                        splitStream.limit(round).forEach(splits::add);
                        // Handle the case where the limit exceeds the partition size.
                        // the limit-bound is more than the partitionSize
                        if (remainder > 0 && size >= round) {
                            splitStream.skip(round).limit(1L)
                                    .map(split ->
                                            split.getSplitByRange(split.getMessagesRange()
                                                    .slice(split.getMessagesRange().getBegin(), remainder)))
                                    .findFirst()
                                    .ifPresent(splits::add);
                        }
                    }
                    else {
                        // Handle the case where the limit is less than or equal to the partition size.
                        final Optional<KafkaSplit> first = repartition.setPartitionSize((int) limit)
                                .stream()
                                .findFirst();
                        if (first.isEmpty()) {
                            continue;
                        }

                        // Calculate the number of rows in the first split.
                        long rows = first.map(KafkaSplit::getMessagesRange)
                                .map(r -> r.getEnd() - r.getBegin())
                                .orElse(0L);
                        totalRows += rows;
                        // Adjust if total rows exceed the limit.
                        if (totalRows > limit) {
                            rows -= totalRows - limit;
                            long finalRows = rows;
                            if (finalRows > 0) {
                                first.map(split ->
                                        // evidently, here the finalRows must be no more than rows
                                        // since we can fetch what we need
                                                split.getSplitByRange(split.getMessagesRange()
                                                        .slice(split.getMessagesRange().getBegin(), toIntExact(finalRows))))
                                        .ifPresent(splits::add);
                            }
                            break;
                        }
                        // Add the first split to the splits list.
                        // there should be no more rows to be shown if the stuff of variable FIRST is not enough
                        first.ifPresent(splits::add);
                    }
                }
                else {
                    // Generate splits for the entire range of each partition if no limit is set.
                    repartition.stream().forEach(splits::add);
                }
            }
            // Return a fixed split source with the collected splits.
            return new FixedSplitSource(splits.build());
        }
        catch (Exception e) { // Catch all exceptions because Kafka library is written in scala and checked exceptions are not declared in method signature.
            if (e instanceof TrinoException) {
                throw e;
            }
            throw new TrinoException(KAFKA_SPLIT_ERROR, format("Cannot list splits for table '%s' reading topic '%s'", kafkaTableHandle.getTableName(), kafkaTableHandle.getTopicName()), e);
        }
    }

    private static TopicPartition toTopicPartition(PartitionInfo partitionInfo)
    {
        return new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    }
}
