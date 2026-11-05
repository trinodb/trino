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
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.predicate.TupleDomain;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    void testSplitsAreGeneratedInBatches()
            throws Exception
    {
        PartitionInfo partition = new PartitionInfo("topic", 0, new Node(0, "broker", 9092), new Node[0], new Node[0]);
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        KafkaSplitSource splitSource = new KafkaSplitSource(
                TABLE,
                ImmutableList.of(partition),
                ImmutableMap.of(topicPartition, 0L),
                ImmutableMap.of(topicPartition, 15L),
                Optional.empty(),
                Optional.empty(),
                10);

        assertThat(splitSource.isFinished()).isFalse();
        assertThat(splitSource.getNextBatch(1, DynamicFilterSnapshot.EMPTY).get())
                .singleElement()
                .isInstanceOfSatisfying(KafkaSplit.class, split -> {
                    assertThat(split.getMessagesRange()).isEqualTo(new Range(0, 10));
                    assertThat(split.getAddresses()).containsExactly(HostAddress.fromParts("broker", 9092));
                });
        assertThat(splitSource.getNextBatch(1, DynamicFilterSnapshot.EMPTY).get())
                .singleElement()
                .isInstanceOfSatisfying(KafkaSplit.class, split -> {
                    assertThat(split.getMessagesRange()).isEqualTo(new Range(10, 15));
                });

        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    void testEmptyPartitionsProducesNoSplits()
            throws Exception
    {
        PartitionInfo partition = new PartitionInfo("topic", 0, new Node(0, "broker", 9092), new Node[0], new Node[0]);
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        KafkaSplitSource splitSource = new KafkaSplitSource(
                TABLE,
                ImmutableList.of(partition),
                ImmutableMap.of(topicPartition, 5L),
                ImmutableMap.of(topicPartition, 5L),
                Optional.empty(),
                Optional.empty(),
                10);

        assertThat(splitSource.getNextBatch(1, DynamicFilterSnapshot.EMPTY).get()).isEmpty();
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    void testMissingLeaderProducesSplitWithoutAddress()
            throws Exception
    {
        PartitionInfo partition = new PartitionInfo("topic", 0, new Node(0, "broker", 9092), new Node[0], new Node[0]);
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        KafkaSplitSource splitSource = new KafkaSplitSource(
                TABLE,
                ImmutableList.of(partition),
                ImmutableMap.of(topicPartition, 0L),
                ImmutableMap.of(topicPartition, 5L),
                Optional.empty(),
                Optional.empty(),
                10);

        List<ConnectorSplit> splits = splitSource.getNextBatch(1, DynamicFilterSnapshot.EMPTY).get();
        assertThat(splits).singleElement().isInstanceOfSatisfying(KafkaSplit.class, split -> assertThat(split.getAddresses()).isEmpty());
    }
}
