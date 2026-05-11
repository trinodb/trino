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
package io.trino.plugin.openlineage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
final class TestOpenLineageEventListenerKafkaIntegration
        extends AbstractTestQueryFramework
{
    private static final Logger logger = Logger.get(TestOpenLineageEventListenerKafkaIntegration.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static TestingKafka testingKafka;
    private static final String TOPIC_NAME = "openlineage-events";
    private static final String TRINO_URI = "http://trino-integration-test:1337";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.create());
        testingKafka.start();
        testingKafka.createTopic(TOPIC_NAME);

        return OpenLineageListenerQueryRunner.builder()
                .addListenerProperty("openlineage-event-listener.transport.type", "KAFKA")
                .addListenerProperty("openlineage-event-listener.kafka-transport.broker-endpoints", testingKafka.getConnectString())
                .addListenerProperty("openlineage-event-listener.kafka-transport.topic-name", TOPIC_NAME)
                .addListenerProperty("openlineage-event-listener.trino.uri", TRINO_URI)
                .build();
    }

    @Test
    void testCreateTableAsSelectFromTable()
    {
        String outputTable = "test_create_table_as_select_from_table";

        @Language("SQL") String createTableQuery = format(
                "CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation",
                outputTable);

        String queryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createTableQuery)
                .queryId()
                .toString();

        assertEventually(Duration.valueOf("10s"), () -> {
            List<JsonNode> events = consumeOpenLineageEvents();
            verifyOpenLineageEvent(events, queryId);
        });
    }

    @Test
    void testCreateTableAsSelectFromView()
    {
        String viewName = "test_view";
        String outputTable = "test_create_table_as_select_from_view";

        @Language("SQL") String createViewQuery = format(
                "CREATE VIEW %s AS SELECT * FROM tpch.tiny.nation",
                viewName);

        assertQuerySucceeds(createViewQuery);

        @Language("SQL") String createTableQuery = format(
                "CREATE TABLE %s AS SELECT * FROM %s",
                outputTable, viewName);

        String queryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createTableQuery)
                .queryId()
                .toString();

        assertEventually(Duration.valueOf("10s"), () -> {
            List<JsonNode> events = consumeOpenLineageEvents();
            verifyOpenLineageEvent(events, queryId);
        });
    }

    private List<JsonNode> consumeOpenLineageEvents()
    {
        List<JsonNode> events = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(List.of(TOPIC_NAME));
            ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.of(5, ChronoUnit.SECONDS));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Consumed OpenLineage event from Kafka: %s", record.value());
                JsonNode event = MAPPER.readTree(record.value());
                events.add(event);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to consume OpenLineage events from Kafka", e);
        }
        return events;
    }

    private KafkaConsumer<String, String> createConsumer()
    {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, testingKafka.getConnectString());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "openlineage-test-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(properties);
    }

    private void verifyOpenLineageEvent(List<JsonNode> events, String expectedQueryId)
    {
        assertThat(events).isNotEmpty();
        logger.info("Verifying %d OpenLineage events for query %s", events.size(), expectedQueryId);

        boolean foundExpectedEvent = false;
        for (JsonNode event : events) {
            for (String field : List.of("eventType", "eventTime", "job", "run")) {
                assertThat(event.has(field)).isTrue().as("Missing field: field=%s", field);
            }

            JsonNode job = event.get("job");
            assertThat(job.has("namespace")).isTrue();
            assertThat(job.has("name")).isTrue();
            String jobName = job.get("name").asText();
            foundExpectedEvent = jobName.contains(expectedQueryId);

            JsonNode run = event.get("run");
            assertThat(run.has("runId")).isTrue();

            String eventType = event.get("eventType").asText();
            logger.info("Event type: %s, Job name: %s", eventType, jobName);
        }
        assertThat(foundExpectedEvent).isTrue();
    }
}
