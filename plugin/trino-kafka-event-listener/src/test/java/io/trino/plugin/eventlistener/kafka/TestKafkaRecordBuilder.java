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

package io.trino.plugin.eventlistener.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.plugin.eventlistener.kafka.metadata.EnvMetadataProvider;
import io.trino.plugin.eventlistener.kafka.metadata.MetadataProvider;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

final class TestKafkaRecordBuilder
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Set<String> EXCLUDED_FIELDS = ImmutableSet.of(
            "ioMetadata",
            "payload",
            "groups",
            "cpuTimeDistribution",
            "operatorSummaries",
            "planNodeStatsAndCosts",
            "stageGcStatistics");
    private static final MetadataProvider TEST_PROVIDER = new TestMetadataProvider("TRINO_INSIGHTS");

    @Test
    void testBuildKafkaRecord()
            throws IOException
    {
        KafkaRecordBuilder builder = new KafkaRecordBuilder("TestQueryStartedEvent", "TestQueryCompletedEvent", "TestSplitCompletedEvent", EXCLUDED_FIELDS, TEST_PROVIDER);
        QueryCompletedEvent queryCompletedEvent = TestUtils.queryCompletedEvent;

        ProducerRecord<String, String> record = builder.buildCompletedRecord(queryCompletedEvent);

        assertThat(record.topic()).isEqualTo("TestQueryCompletedEvent");
        assertThat(record.key()).isNull();

        JsonNode jsonNode = MAPPER.readTree(record.value()).get("eventPayload");
        checkFieldsExcluded(jsonNode);
    }

    @Test
    void testBuildKafkaRecordWithExclusions()
            throws IOException
    {
        Set<String> exclude = Sets.union(EXCLUDED_FIELDS, Set.of("query", "principal", "analysisTime", "writtenBytes"));
        KafkaRecordBuilder builder = new KafkaRecordBuilder("TestQueryStartedEvent", "TestQueryCompletedEvent", "TestSplitCompletedEvent", exclude, TEST_PROVIDER);
        QueryCompletedEvent queryCompletedEvent = TestUtils.queryCompletedEvent;

        ProducerRecord<String, String> record = builder.buildCompletedRecord(queryCompletedEvent);

        assertThat(record.topic()).isEqualTo("TestQueryCompletedEvent");
        assertThat(record.key()).isNull();

        JsonNode jsonNode = MAPPER.readTree(record.value()).get("eventPayload");
        checkFieldsExcluded(jsonNode);
        assertThat(jsonNode.get("metadata").get("query")).isNull();
        assertThat(jsonNode.get("context").get("principal")).isNull();
        assertThat(jsonNode.get("statistics").get("analysisTime")).isNull();
        assertThat(jsonNode.get("statistics").get("writtenBytes")).isNull();
    }

    @Test
    void testBuildKafkaRecordWithMetadata()
            throws IOException
    {
        Set<String> exclude = Sets.union(EXCLUDED_FIELDS, Set.of("context", "payload", "analysisTime"));
        KafkaRecordBuilder builder = new KafkaRecordBuilder("TestQueryStartedEvent", "TestQueryCompletedEvent", "TestSplitCompletedEvent", exclude, TEST_PROVIDER);
        QueryCompletedEvent queryCompletedEvent = TestUtils.queryCompletedEvent;

        ProducerRecord<String, String> record = builder.buildCompletedRecord(queryCompletedEvent);

        assertThat(record.topic()).isEqualTo("TestQueryCompletedEvent");
        assertThat(record.key()).isNull();
        Map<String, String> metadata = MAPPER.readValue(MAPPER.readTree(record.value()).get("eventMetadata").toString(), Map.class);

        assertThat(metadata).containsExactly(entry("baz", "yoo"));
    }

    static class TestMetadataProvider
            extends EnvMetadataProvider
    {
        public TestMetadataProvider(String prefix)
        {
            super(prefix);
        }

        @Override
        public Map<String, String> getMetadata()
        {
            return filter(Map.of("foo", "bar", prefix + "baz", "yoo"));
        }
    }

    private static void checkFieldsExcluded(JsonNode jsonNode)
    {
        assertThat(jsonNode.get("ioMetadata")).isNull();
        assertThat(jsonNode.get("context").isNull()).isFalse();
        assertThat(jsonNode.get("context").get("user").isNull()).isFalse();
        assertThat(jsonNode.get("context").get("groups")).isNull();
        assertThat(jsonNode.get("metadata").isNull()).isFalse();
        assertThat(jsonNode.get("metadata").get("queryId").isNull()).isFalse();
        assertThat(jsonNode.get("metadata").get("payload")).isNull();
        assertThat(jsonNode.get("statistics").isNull()).isFalse();
        assertThat(jsonNode.get("statistics").get("totalBytes").isNull()).isFalse();
        assertThat(jsonNode.get("statistics").get("stageGcStatistics")).isNull();
        assertThat(jsonNode.get("statistics").get("planNodeStatsAndCosts")).isNull();
        assertThat(jsonNode.get("statistics").get("operatorSummaries")).isNull();
        assertThat(jsonNode.get("statistics").get("cpuTimeDistribution")).isNull();
    }
}
