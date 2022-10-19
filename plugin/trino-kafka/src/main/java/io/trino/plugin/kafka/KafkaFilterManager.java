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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

import javax.inject.Inject;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.OFFSET_TIMESTAMP_FIELD;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.PARTITION_ID_FIELD;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.PARTITION_OFFSET_FIELD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.lang.Math.floorDiv;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KafkaFilterManager
{
    private static final long INVALID_KAFKA_RANGE_INDEX = -1;
    private static final String TOPIC_CONFIG_TIMESTAMP_KEY = "message.timestamp.type";
    private static final String TOPIC_CONFIG_TIMESTAMP_VALUE_LOG_APPEND_TIME = "LogAppendTime";

    private final KafkaConsumerFactory consumerFactory;
    private final KafkaAdminFactory adminFactory;
    private final KafkaInternalFieldManager kafkaInternalFieldManager;

    @Inject
    public KafkaFilterManager(KafkaConsumerFactory consumerFactory, KafkaAdminFactory adminFactory, KafkaInternalFieldManager kafkaInternalFieldManager)
    {
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory is null");
        this.adminFactory = requireNonNull(adminFactory, "adminFactory is null");
        this.kafkaInternalFieldManager = requireNonNull(kafkaInternalFieldManager, "kafkaInternalFieldManager is null");
    }

    public KafkaFilteringResult getKafkaFilterResult(
            ConnectorSession session,
            KafkaTableHandle kafkaTableHandle,
            List<PartitionInfo> partitionInfos,
            Map<TopicPartition, Long> partitionBeginOffsets,
            Map<TopicPartition, Long> partitionEndOffsets)
    {
        requireNonNull(session, "session is null");
        requireNonNull(kafkaTableHandle, "kafkaTableHandle is null");
        requireNonNull(partitionInfos, "partitionInfos is null");
        requireNonNull(partitionBeginOffsets, "partitionBeginOffsets is null");
        requireNonNull(partitionEndOffsets, "partitionEndOffsets is null");

        TupleDomain<ColumnHandle> constraint = kafkaTableHandle.getConstraint();
        verify(!constraint.isNone(), "constraint is none");

        if (!constraint.isAll()) {
            Set<Long> partitionIds = partitionInfos.stream().map(partitionInfo -> (long) partitionInfo.partition()).collect(toImmutableSet());

            Map<String, Domain> domains = constraint.getDomains().orElseThrow()
                    .entrySet().stream()
                    .collect(toImmutableMap(
                            entry -> ((KafkaColumnHandle) entry.getKey()).getName(),
                            Map.Entry::getValue));

            Optional<Range> offsetRanged = getDomain(PARTITION_OFFSET_FIELD, domains)
                    .flatMap(KafkaFilterManager::filterRangeByDomain);
            Set<Long> partitionIdsFiltered = getDomain(PARTITION_ID_FIELD, domains)
                    .map(domain -> filterValuesByDomain(domain, partitionIds))
                    .orElse(partitionIds);
            Optional<Range> offsetTimestampRanged = getDomain(OFFSET_TIMESTAMP_FIELD, domains)
                    .flatMap(KafkaFilterManager::filterRangeByDomain);

            // push down offset
            if (offsetRanged.isPresent()) {
                Range range = offsetRanged.get();
                partitionBeginOffsets = overridePartitionBeginOffsets(partitionBeginOffsets,
                        partition -> (range.getBegin() != INVALID_KAFKA_RANGE_INDEX) ? Optional.of(range.getBegin()) : Optional.empty());
                partitionEndOffsets = overridePartitionEndOffsets(partitionEndOffsets,
                        partition -> (range.getEnd() != INVALID_KAFKA_RANGE_INDEX) ? Optional.of(range.getEnd()) : Optional.empty());
            }

            // push down timestamp if possible
            if (offsetTimestampRanged.isPresent()) {
                try (KafkaConsumer<byte[], byte[]> kafkaConsumer = consumerFactory.create(session)) {
                    // filter negative value to avoid java.lang.IllegalArgumentException when using KafkaConsumer offsetsForTimes
                    if (offsetTimestampRanged.get().getBegin() > INVALID_KAFKA_RANGE_INDEX) {
                        partitionBeginOffsets = overridePartitionBeginOffsets(partitionBeginOffsets,
                                partition -> findOffsetsForTimestampGreaterOrEqual(kafkaConsumer, partition, offsetTimestampRanged.get().getBegin()));
                    }
                    if (isTimestampUpperBoundPushdownEnabled(session, kafkaTableHandle.getTopicName())) {
                        if (offsetTimestampRanged.get().getEnd() > INVALID_KAFKA_RANGE_INDEX) {
                            partitionEndOffsets = overridePartitionEndOffsets(partitionEndOffsets,
                                    partition -> findOffsetsForTimestampGreaterOrEqual(kafkaConsumer, partition, offsetTimestampRanged.get().getEnd()));
                        }
                    }
                }
            }

            // push down partitions
            List<PartitionInfo> partitionFilteredInfos = partitionInfos.stream()
                    .filter(partitionInfo -> partitionIdsFiltered.contains((long) partitionInfo.partition()))
                    .collect(toImmutableList());
            return new KafkaFilteringResult(partitionFilteredInfos, partitionBeginOffsets, partitionEndOffsets);
        }
        return new KafkaFilteringResult(partitionInfos, partitionBeginOffsets, partitionEndOffsets);
    }

    private Optional<Domain> getDomain(InternalFieldId internalFieldId, Map<String, Domain> columnNameToDomain)
    {
        String columnName = kafkaInternalFieldManager.getFieldById(internalFieldId).getColumnName();
        return Optional.ofNullable(columnNameToDomain.get(columnName));
    }

    private boolean isTimestampUpperBoundPushdownEnabled(ConnectorSession session, String topic)
    {
        try (Admin adminClient = adminFactory.create(session)) {
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

            DescribeConfigsResult describeResult = adminClient.describeConfigs(Collections.singleton(topicResource));
            Map<ConfigResource, Config> configMap = describeResult.all().get();

            if (configMap != null) {
                Config config = configMap.get(topicResource);
                String timestampType = config.get(TOPIC_CONFIG_TIMESTAMP_KEY).value();
                if (TOPIC_CONFIG_TIMESTAMP_VALUE_LOG_APPEND_TIME.equals(timestampType)) {
                    return true;
                }
            }
        }
        catch (Exception e) {
            throw new TrinoException(KAFKA_SPLIT_ERROR, format("Failed to get configuration for topic '%s'", topic), e);
        }
        return KafkaSessionProperties.isTimestampUpperBoundPushdownEnabled(session);
    }

    private static Optional<Long> findOffsetsForTimestampGreaterOrEqual(KafkaConsumer<byte[], byte[]> kafkaConsumer, TopicPartition topicPartition, long timestamp)
    {
        final long transferTimestamp = floorDiv(timestamp, MICROSECONDS_PER_MILLISECOND);
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsets = kafkaConsumer.offsetsForTimes(ImmutableMap.of(topicPartition, transferTimestamp));
        return Optional.ofNullable(getOnlyElement(topicPartitionOffsets.values(), null)).map(OffsetAndTimestamp::offset);
    }

    private static Map<TopicPartition, Long> overridePartitionBeginOffsets(Map<TopicPartition, Long> partitionBeginOffsets,
            Function<TopicPartition, Optional<Long>> overrideFunction)
    {
        ImmutableMap.Builder<TopicPartition, Long> partitionFilteredBeginOffsetsBuilder = ImmutableMap.builder();
        partitionBeginOffsets.forEach((partition, partitionIndex) -> {
            Optional<Long> newOffset = overrideFunction.apply(partition);
            partitionFilteredBeginOffsetsBuilder.put(partition, newOffset.map(index -> Long.max(partitionIndex, index)).orElse(partitionIndex));
        });
        return partitionFilteredBeginOffsetsBuilder.buildOrThrow();
    }

    private static Map<TopicPartition, Long> overridePartitionEndOffsets(Map<TopicPartition, Long> partitionEndOffsets,
            Function<TopicPartition, Optional<Long>> overrideFunction)
    {
        ImmutableMap.Builder<TopicPartition, Long> partitionFilteredEndOffsetsBuilder = ImmutableMap.builder();
        partitionEndOffsets.forEach((partition, partitionIndex) -> {
            Optional<Long> newOffset = overrideFunction.apply(partition);
            partitionFilteredEndOffsetsBuilder.put(partition, newOffset.map(index -> Long.min(partitionIndex, index)).orElse(partitionIndex));
        });
        return partitionFilteredEndOffsetsBuilder.buildOrThrow();
    }

    @VisibleForTesting
    public static Optional<Range> filterRangeByDomain(Domain domain)
    {
        Long low = INVALID_KAFKA_RANGE_INDEX;
        Long high = INVALID_KAFKA_RANGE_INDEX;
        if (domain.isSingleValue()) {
            // still return range for single value case like (_partition_offset=XXX or _timestamp=XXX)
            low = (long) domain.getSingleValue();
            high = (long) domain.getSingleValue();
        }
        else {
            ValueSet valueSet = domain.getValues();
            if (valueSet instanceof SortedRangeSet) {
                // still return range for single value case like (_partition_offset in (XXX1,XXX2) or _timestamp in XXX1, XXX2)
                Ranges ranges = ((SortedRangeSet) valueSet).getRanges();
                List<io.trino.spi.predicate.Range> rangeList = ranges.getOrderedRanges();
                if (rangeList.stream().allMatch(io.trino.spi.predicate.Range::isSingleValue)) {
                    List<Long> values = rangeList.stream()
                            .map(range -> (Long) range.getSingleValue())
                            .collect(toImmutableList());
                    low = Collections.min(values);
                    high = Collections.max(values);
                }
                else {
                    io.trino.spi.predicate.Range span = ranges.getSpan();
                    low = getLowIncludedValue(span).orElse(low);
                    high = getHighIncludedValue(span).orElse(high);
                }
            }
        }
        if (high != INVALID_KAFKA_RANGE_INDEX) {
            high = high + 1;
        }
        return Optional.of(new Range(low, high));
    }

    @VisibleForTesting
    public static Set<Long> filterValuesByDomain(Domain domain, Set<Long> sourceValues)
    {
        requireNonNull(sourceValues, "sourceValues is none");
        if (domain.isSingleValue()) {
            long singleValue = (long) domain.getSingleValue();
            return sourceValues.stream().filter(sourceValue -> sourceValue == singleValue).collect(toImmutableSet());
        }
        ValueSet valueSet = domain.getValues();
        if (valueSet instanceof SortedRangeSet) {
            Ranges ranges = ((SortedRangeSet) valueSet).getRanges();
            List<io.trino.spi.predicate.Range> rangeList = ranges.getOrderedRanges();
            if (rangeList.stream().allMatch(io.trino.spi.predicate.Range::isSingleValue)) {
                return rangeList.stream()
                        .map(range -> (Long) range.getSingleValue())
                        .filter(sourceValues::contains)
                        .collect(toImmutableSet());
            }
            // still return values for range case like (_partition_id > 1)
            io.trino.spi.predicate.Range span = ranges.getSpan();
            long low = getLowIncludedValue(span).orElse(0L);
            long high = getHighIncludedValue(span).orElse(Long.MAX_VALUE);
            return sourceValues.stream()
                    .filter(item -> item >= low && item <= high)
                    .collect(toImmutableSet());
        }
        return sourceValues;
    }

    private static Optional<Long> getLowIncludedValue(io.trino.spi.predicate.Range range)
    {
        long step = nativeRepresentationGranularity(range.getType());
        return range.getLowValue()
                .map(Long.class::cast)
                .map(value -> range.isLowInclusive() ? value : value + step);
    }

    private static Optional<Long> getHighIncludedValue(io.trino.spi.predicate.Range range)
    {
        long step = nativeRepresentationGranularity(range.getType());
        return range.getHighValue()
                .map(Long.class::cast)
                .map(value -> range.isHighInclusive() ? value : value - step);
    }

    private static long nativeRepresentationGranularity(Type type)
    {
        if (type == BIGINT) {
            return 1;
        }
        if (type instanceof TimestampType && ((TimestampType) type).getPrecision() == 3) {
            // native representation is in microseconds
            return 1000;
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
}
