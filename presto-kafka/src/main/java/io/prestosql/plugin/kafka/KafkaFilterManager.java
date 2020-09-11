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
package io.prestosql.plugin.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Ranges;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import org.apache.kafka.clients.admin.AdminClient;
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
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static io.prestosql.plugin.kafka.KafkaInternalFieldManager.OFFSET_TIMESTAMP_FIELD;
import static io.prestosql.plugin.kafka.KafkaInternalFieldManager.PARTITION_ID_FIELD;
import static io.prestosql.plugin.kafka.KafkaInternalFieldManager.PARTITION_OFFSET_FIELD;
import static io.prestosql.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
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

    @Inject
    public KafkaFilterManager(KafkaConsumerFactory consumerFactory, KafkaAdminFactory adminFactory)
    {
        this.consumerFactory = requireNonNull(consumerFactory, "consumerManager is null");
        this.adminFactory = requireNonNull(adminFactory, "adminFactory is null");
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
            Optional<Range> offsetRanged = Optional.empty();
            Optional<Range> offsetTimestampRanged = Optional.empty();
            Set<Long> partitionIdsFiltered = partitionIds;
            Optional<Map<ColumnHandle, Domain>> domains = constraint.getDomains();

            for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
                KafkaColumnHandle columnHandle = (KafkaColumnHandle) entry.getKey();
                if (!columnHandle.isInternal()) {
                    continue;
                }
                switch (columnHandle.getName()) {
                    case PARTITION_OFFSET_FIELD:
                        offsetRanged = filterRangeByDomain(entry.getValue());
                        break;
                    case PARTITION_ID_FIELD:
                        partitionIdsFiltered = filterValuesByDomain(entry.getValue(), partitionIds);
                        break;
                    case OFFSET_TIMESTAMP_FIELD:
                        offsetTimestampRanged = filterRangeByDomain(entry.getValue());
                        break;
                    default:
                        break;
                }
            }

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
                try (KafkaConsumer<byte[], byte[]> kafkaConsumer = consumerFactory.create()) {
                    Optional<Range> finalOffsetTimestampRanged = offsetTimestampRanged;
                    partitionBeginOffsets = overridePartitionBeginOffsets(partitionBeginOffsets,
                            partition -> findOffsetsForTimestampGreaterOrEqual(kafkaConsumer, partition, finalOffsetTimestampRanged.get().getBegin()));
                    if (isTimestampUpperBoundPushdownEnabled(session, kafkaTableHandle.getTopicName())) {
                        partitionEndOffsets = overridePartitionEndOffsets(partitionEndOffsets,
                                partition -> findOffsetsForTimestampGreaterOrEqual(kafkaConsumer, partition, finalOffsetTimestampRanged.get().getEnd()));
                    }
                }
            }

            // push down partitions
            final Set<Long> finalPartitionIdsFiltered = partitionIdsFiltered;
            List<PartitionInfo> partitionFilteredInfos = partitionInfos.stream()
                    .filter(partitionInfo -> finalPartitionIdsFiltered.contains((long) partitionInfo.partition()))
                    .collect(toImmutableList());
            return new KafkaFilteringResult(partitionFilteredInfos, partitionBeginOffsets, partitionEndOffsets);
        }
        return new KafkaFilteringResult(partitionInfos, partitionBeginOffsets, partitionEndOffsets);
    }

    private boolean isTimestampUpperBoundPushdownEnabled(ConnectorSession session, String topic)
    {
        try (AdminClient adminClient = adminFactory.create()) {
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
            throw new PrestoException(KAFKA_SPLIT_ERROR, format("Failed to get configuration for topic '%s'", topic), e);
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
        return partitionFilteredBeginOffsetsBuilder.build();
    }

    private static Map<TopicPartition, Long> overridePartitionEndOffsets(Map<TopicPartition, Long> partitionEndOffsets,
                                                                         Function<TopicPartition, Optional<Long>> overrideFunction)
    {
        ImmutableMap.Builder<TopicPartition, Long> partitionFilteredEndOffsetsBuilder = ImmutableMap.builder();
        partitionEndOffsets.forEach((partition, partitionIndex) -> {
            Optional<Long> newOffset = overrideFunction.apply(partition);
            partitionFilteredEndOffsetsBuilder.put(partition, newOffset.map(index -> Long.min(partitionIndex, index)).orElse(partitionIndex));
        });
        return partitionFilteredEndOffsetsBuilder.build();
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
                List<io.prestosql.spi.predicate.Range> rangeList = ranges.getOrderedRanges();
                if (rangeList.stream().allMatch(io.prestosql.spi.predicate.Range::isSingleValue)) {
                    List<Long> values = rangeList.stream()
                            .map(range -> (Long) range.getSingleValue())
                            .collect(toImmutableList());
                    low = Collections.min(values);
                    high = Collections.max(values);
                }
                else {
                    Marker lowMark = ranges.getSpan().getLow();
                    low = getLowByLowMark(lowMark).orElse(low);
                    Marker highMark = ranges.getSpan().getHigh();
                    high = getHighByHighMark(highMark).orElse(high);
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
        else {
            ValueSet valueSet = domain.getValues();
            if (valueSet instanceof SortedRangeSet) {
                Ranges ranges = ((SortedRangeSet) valueSet).getRanges();
                List<io.prestosql.spi.predicate.Range> rangeList = ranges.getOrderedRanges();
                if (rangeList.stream().allMatch(io.prestosql.spi.predicate.Range::isSingleValue)) {
                    return rangeList.stream()
                            .map(range -> (Long) range.getSingleValue())
                            .filter(sourceValues::contains)
                            .collect(toImmutableSet());
                }
                else {
                    // still return values for range case like (_partition_id > 1)
                    long low = 0;
                    long high = Long.MAX_VALUE;

                    Marker lowMark = ranges.getSpan().getLow();
                    low = maxLow(low, lowMark);
                    Marker highMark = ranges.getSpan().getHigh();
                    high = minHigh(high, highMark);
                    final long finalLow = low;
                    final long finalHigh = high;
                    return sourceValues.stream()
                            .filter(item -> item >= finalLow && item <= finalHigh)
                            .collect(toImmutableSet());
                }
            }
        }
        return sourceValues;
    }

    private static long minHigh(long high, Marker highMark)
    {
        Optional<Long> highByHighMark = getHighByHighMark(highMark);
        if (highByHighMark.isPresent()) {
            high = Long.min(highByHighMark.get(), high);
        }
        return high;
    }

    private static long maxLow(long low, Marker lowMark)
    {
        Optional<Long> lowByLowMark = getLowByLowMark(lowMark);
        if (lowByLowMark.isPresent()) {
            low = Long.max(lowByLowMark.get(), low);
        }
        return low;
    }

    private static Optional<Long> getHighByHighMark(Marker highMark)
    {
        if (!highMark.isUpperUnbounded()) {
            long high = (Long) highMark.getValue();
            switch (highMark.getBound()) {
                case EXACTLY:
                    break;
                case BELOW:
                    high--;
                    break;
                default:
                    throw new AssertionError("Unhandled bound: " + highMark.getBound());
            }
            return Optional.of(high);
        }
        return Optional.empty();
    }

    private static Optional<Long> getLowByLowMark(Marker lowMark)
    {
        if (!lowMark.isLowerUnbounded()) {
            long low = (Long) lowMark.getValue();
            switch (lowMark.getBound()) {
                case EXACTLY:
                    break;
                case ABOVE:
                    low++;
                    break;
                default:
                    throw new AssertionError("Unhandled bound: " + lowMark.getBound());
            }
            return Optional.of(low);
        }
        return Optional.empty();
    }
}
