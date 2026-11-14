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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.predicate.TupleDomain;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestKafkaSplitSource
{
    private static final KafkaTableHandle TABLE = new KafkaTableHandle(
            "default",
            "table",
            "topic",
            "raw",
            "raw",
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            ImmutableList.of(),
            TupleDomain.all());

    private static final Node LEADER = new Node(0, "broker", 9092);

    @Test
    void testSplitsAreGeneratedInBatches()
            throws Exception
    {
        KafkaSplitSource splitSource = splitSource(
                ImmutableList.of(partitionInfo(0, LEADER)),
                ImmutableMap.of(topicPartition(0), 0L),
                ImmutableMap.of(topicPartition(0), 15L),
                10);

        assertThat(splitSource.isFinished()).isFalse();
        assertThat(nextBatch(splitSource, 1))
                .singleElement()
                .isInstanceOfSatisfying(KafkaSplit.class, split -> {
                    assertThat(split.getMessagesRange()).isEqualTo(new Range(0, 10));
                    assertThat(split.getAddresses()).containsExactly(HostAddress.fromParts("broker", 9092));
                });
        assertThat(nextBatch(splitSource, 1))
                .singleElement()
                .isInstanceOfSatisfying(KafkaSplit.class, split ->
                        assertThat(split.getMessagesRange()).isEqualTo(new Range(10, 15)));

        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    void testRespectsMaxBatchSize()
            throws Exception
    {
        KafkaSplitSource splitSource = splitSource(
                ImmutableList.of(partitionInfo(0, LEADER)),
                ImmutableMap.of(topicPartition(0), 0L),
                ImmutableMap.of(topicPartition(0), 30L),
                10);

        assertThat(nextBatch(splitSource, 2))
                .extracting(KafkaSplit::getMessagesRange)
                .containsExactly(
                        new Range(0, 10),
                        new Range(10, 20));
        assertThat(splitSource.isFinished()).isFalse();

        assertThat(nextBatch(splitSource, 2))
                .extracting(KafkaSplit::getMessagesRange)
                .containsExactly(new Range(20, 30));
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    void testEmptyPartitionsAreSkipped()
            throws Exception
    {
        KafkaSplitSource splitSource = splitSource(
                ImmutableList.of(partitionInfo(0, LEADER), partitionInfo(1, LEADER)),
                ImmutableMap.of(
                        topicPartition(0), 5L,
                        topicPartition(1), 0L),
                ImmutableMap.of(
                        topicPartition(0), 5L,
                        topicPartition(1), 12L),
                5);

        // isFinished() may advance past empty partitions, but must not consume any actual range
        assertThat(splitSource.isFinished()).isFalse();
        assertThat(splitSource.isFinished()).isFalse();

        List<KafkaSplit> splits = nextBatch(splitSource, 10);
        assertThat(splits)
                .extracting(KafkaSplit::getPartitionId)
                .containsExactly(1, 1, 1);
        assertThat(splits)
                .extracting(KafkaSplit::getMessagesRange)
                .containsExactly(
                        new Range(0, 5),
                        new Range(5, 10),
                        new Range(10, 12));
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    void testSplitsAcrossPartitions()
            throws Exception
    {
        Node firstLeader = new Node(1, "broker1", 9092);
        Node secondLeader = new Node(2, "broker2", 9092);
        KafkaSplitSource splitSource = splitSource(
                ImmutableList.of(partitionInfo(0, firstLeader), partitionInfo(1, secondLeader)),
                ImmutableMap.of(
                        topicPartition(0), 0L,
                        topicPartition(1), 100L),
                ImmutableMap.of(
                        topicPartition(0), 10L,
                        topicPartition(1), 105L),
                10);

        List<KafkaSplit> splits = nextBatch(splitSource, 10);
        assertThat(splits).hasSize(2);
        assertThat(splits.get(0).getPartitionId()).isEqualTo(0);
        assertThat(splits.get(0).getMessagesRange()).isEqualTo(new Range(0, 10));
        assertThat(splits.get(0).getAddresses()).containsExactly(HostAddress.fromParts("broker1", 9092));
        assertThat(splits.get(1).getPartitionId()).isEqualTo(1);
        assertThat(splits.get(1).getMessagesRange()).isEqualTo(new Range(100, 105));
        assertThat(splits.get(1).getAddresses()).containsExactly(HostAddress.fromParts("broker2", 9092));
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    void testAllPartitionsEmptyProducesNoSplits()
            throws Exception
    {
        KafkaSplitSource splitSource = splitSource(
                ImmutableList.of(partitionInfo(0, LEADER)),
                ImmutableMap.of(topicPartition(0), 5L),
                ImmutableMap.of(topicPartition(0), 5L),
                10);

        assertThat(nextBatch(splitSource, 1)).isEmpty();
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    void testMissingLeaderProducesSplitWithoutAddress()
            throws Exception
    {
        KafkaSplitSource splitSource = splitSource(
                ImmutableList.of(partitionInfo(0, null)),
                ImmutableMap.of(topicPartition(0), 0L),
                ImmutableMap.of(topicPartition(0), 5L),
                10);

        assertThat(nextBatch(splitSource, 1))
                .singleElement()
                .isInstanceOfSatisfying(KafkaSplit.class, split -> assertThat(split.getAddresses()).isEmpty());
    }

    @Test
    void testMissingBeginningOffsetFails()
    {
        KafkaSplitSource splitSource = splitSource(
                ImmutableList.of(partitionInfo(0, LEADER)),
                ImmutableMap.of(),
                ImmutableMap.of(topicPartition(0), 5L),
                10);

        assertThatThrownBy(() -> splitSource.getNextBatch(1, DynamicFilterSnapshot.EMPTY))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Missing beginning offset for partition");
    }

    @Test
    void testMissingEndOffsetFails()
    {
        KafkaSplitSource splitSource = splitSource(
                ImmutableList.of(partitionInfo(0, LEADER)),
                ImmutableMap.of(topicPartition(0), 0L),
                ImmutableMap.of(),
                10);

        assertThatThrownBy(() -> splitSource.getNextBatch(1, DynamicFilterSnapshot.EMPTY))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Missing end offset for partition");
    }

    @Test
    void testClosedSplitSourceProducesNoSplits()
            throws Exception
    {
        KafkaSplitSource splitSource = splitSource(
                ImmutableList.of(partitionInfo(0, LEADER)),
                ImmutableMap.of(topicPartition(0), 0L),
                ImmutableMap.of(topicPartition(0), 5L),
                10);

        splitSource.close();

        assertThat(splitSource.isFinished()).isTrue();
        assertThat(nextBatch(splitSource, 1)).isEmpty();
    }

    private static KafkaSplitSource splitSource(
            List<PartitionInfo> partitionInfos,
            Map<TopicPartition, Long> beginOffsets,
            Map<TopicPartition, Long> endOffsets,
            int messagesPerSplit)
    {
        return new KafkaSplitSource(
                TABLE,
                partitionInfos,
                beginOffsets,
                endOffsets,
                Optional.empty(),
                Optional.empty(),
                messagesPerSplit);
    }

    private static PartitionInfo partitionInfo(int partitionId, Node leader)
    {
        return new PartitionInfo("topic", partitionId, leader, new Node[0], new Node[0]);
    }

    private static TopicPartition topicPartition(int partitionId)
    {
        return new TopicPartition("topic", partitionId);
    }

    private static List<KafkaSplit> nextBatch(KafkaSplitSource splitSource, int maxSize)
            throws Exception
    {
        return splitSource.getNextBatch(maxSize, DynamicFilterSnapshot.EMPTY)
                .get()
                .stream()
                .map(KafkaSplit.class::cast)
                .collect(toImmutableList());
    }
}
