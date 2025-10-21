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

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import io.trino.testing.TestingConnectorSession;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.kafka.KafkaSessionProperties.KAFKA_TOPIC_PARTITION_OFFSET_OVERRIDES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOverriddenTopicPartitionOffsetProvider
{
    @Test
    public void testCanParseOverriddenOffsets()
    {
        KafkaPartitionInputs inputs = OverriddenTopicPartitionOffsetProvider.parseOverriddenInputs("x-0=0-10, y-1=5-20 ");
        assertThat(inputs.partitionBeginOffsets().get(new TopicPartition("x", 0))).isEqualTo(0);
        assertThat(inputs.partitionEndOffsets().get(new TopicPartition("x", 0))).isEqualTo(10);
        assertThat(inputs.partitionBeginOffsets().get(new TopicPartition("y", 1))).isEqualTo(5);
        assertThat(inputs.partitionEndOffsets().get(new TopicPartition("y", 1))).isEqualTo(20);
    }

    @Test
    public void testCanOverrideProvidedOffsets()
    {
        String offsetOverrides = "x-0=5-10, y-1=5-20 ";
        ConnectorSession connectorSession = TestingConnectorSession.builder().setPropertyMetadata(List.of(PropertyMetadata.stringProperty(
                KAFKA_TOPIC_PARTITION_OFFSET_OVERRIDES,
                "Test description",
                offsetOverrides, false))).build();
        String topicName = StaticTopicPartitionOffsetProvider.TOPIC_PARTITION.topic();
        OverriddenTopicPartitionOffsetProvider victim = new OverriddenTopicPartitionOffsetProvider(new StaticTopicPartitionOffsetProvider());
        KafkaPartitionInputs inputs = victim.offsetRangesPerPartition(null, topicName, connectorSession);
        assertThat(inputs.partitionInfos()).containsOnly(StaticTopicPartitionOffsetProvider.PARTITION_INFO);
        assertThat(inputs.partitionBeginOffsets()).containsExactlyEntriesOf(Map.of(StaticTopicPartitionOffsetProvider.TOPIC_PARTITION, 5L));
        assertThat(inputs.partitionEndOffsets()).containsExactlyEntriesOf(Map.of(StaticTopicPartitionOffsetProvider.TOPIC_PARTITION, 10L));
    }

    @Test
    public void testThrowsTrinoExceptionOnMalformedTopicPartitionOverrides()
    {
        assertThatThrownBy(() -> OverriddenTopicPartitionOffsetProvider.parseOverriddenInputs("x-0?0-10, y-1=5-20 ")).isInstanceOf(TrinoException.class);
        assertThatThrownBy(() -> OverriddenTopicPartitionOffsetProvider.parseOverriddenInputs("x:0=0-10, y-1=5-20 ")).isInstanceOf(TrinoException.class);
        assertThatThrownBy(() -> OverriddenTopicPartitionOffsetProvider.parseOverriddenInputs("x-a=0-10, y-1=5-20 ")).isInstanceOf(TrinoException.class);
        assertThatThrownBy(() -> OverriddenTopicPartitionOffsetProvider.parseOverriddenInputs("x-0=a-10, y-1=5-20 ")).isInstanceOf(TrinoException.class);
        assertThatThrownBy(() -> OverriddenTopicPartitionOffsetProvider.parseOverriddenInputs("x-0=0-a, y-1=5-20 ")).isInstanceOf(TrinoException.class);
    }

    private static class StaticTopicPartitionOffsetProvider
            implements TopicPartitionOffsetProvider
    {
        public static final TopicPartition TOPIC_PARTITION = new TopicPartition("x", 0);
        public static final PartitionInfo PARTITION_INFO = new PartitionInfo(TOPIC_PARTITION.topic(), TOPIC_PARTITION.partition(), Node.noNode(), new Node[0], new Node[0], new Node[0]);

        @Override
        public KafkaPartitionInputs offsetRangesPerPartition(KafkaConsumer<byte[], byte[]> kafkaConsumer, String topicName, ConnectorSession session)
        {
            return new KafkaPartitionInputs(List.of(PARTITION_INFO), Map.of(TOPIC_PARTITION, 0L), Map.of(TOPIC_PARTITION, 20L));
        }
    }
}
