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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.testing.kafka.TestingKafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

final class TestKafkaEventListenerPlugin
{
    private static final String CREATED_TOPIC = "query_created";
    private static final String COMPLETED_TOPIC = "query_completed";
    private static final String SPLIT_COMPLETED_TOPIC = "split_completed";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static TestingKafka testingKafka;

    @BeforeAll
    public static void setUp()
    {
        testingKafka = TestingKafka.create();
        testingKafka.start();
        testingKafka.createTopic(CREATED_TOPIC);
        testingKafka.createTopic(COMPLETED_TOPIC);
        testingKafka.createTopic(SPLIT_COMPLETED_TOPIC);
    }

    @AfterAll
    public static void teardown()
            throws IOException
    {
        testingKafka.close();
    }

    @Test
    void testEventListenerEndToEnd()
            throws JsonProcessingException
    {
        KafkaEventListenerPlugin plugin = new KafkaEventListenerPlugin();

        EventListenerFactory factory = getOnlyElement(plugin.getEventListenerFactories());
        EventListener eventListener = factory.create(
                ImmutableMap.<String, String>builder()
                        .put("kafka-event-listener.publish-created-event", "true")
                        .put("kafka-event-listener.publish-completed-event", "true")
                        .put("kafka-event-listener.publish-split-completed-event", "true")
                        .put("kafka-event-listener.broker-endpoints", testingKafka.getConnectString())
                        .put("kafka-event-listener.created-event.topic", CREATED_TOPIC)
                        .put("kafka-event-listener.completed-event.topic", COMPLETED_TOPIC)
                        .put("kafka-event-listener.split-completed-event.topic", SPLIT_COMPLETED_TOPIC)
                        .put("kafka-event-listener.env-var-prefix", "INSIGHTS_")
                        .put("kafka-event-listener.request-timeout", "30s")
                        .put("kafka-event-listener.excluded-fields", "ioMetadata")
                        .put("kafka.security-protocol", "plaintext")
                        .buildOrThrow());

        try {
            // produce and consume a test created event
            eventListener.queryCreated(TestUtils.queryCreatedEvent);
            ConsumerRecord<String, String> record = getOnlyElement(pollJsonRecords(CREATED_TOPIC));
            JsonNode jsonNode = MAPPER.readTree(record.value());
            assertThat(jsonNode.get("eventMetadata")).isNotNull();
            JsonNode jsonEvent = jsonNode.get("eventPayload");
            assertThat(jsonEvent).isNotNull();
            assertThat(jsonEvent.get("context")).isNotNull();
            assertThat(jsonEvent.get("metadata")).isNotNull();
            assertThat(jsonEvent.get("metadata").get("queryId").textValue()).isEqualTo(TestUtils.queryCreatedEvent.getMetadata().getQueryId());

            // produce and consume a test completed event
            eventListener.queryCompleted(TestUtils.queryCompletedEvent);
            record = getOnlyElement(pollJsonRecords(COMPLETED_TOPIC));
            jsonNode = MAPPER.readTree(record.value());
            assertThat(jsonNode.get("eventMetadata")).isNotNull();
            jsonEvent = jsonNode.get("eventPayload");
            assertThat(jsonEvent).isNotNull();
            assertThat(jsonEvent.get("context")).isNotNull();
            assertThat(jsonEvent.get("metadata")).isNotNull();
            assertThat(jsonEvent.get("statistics")).isNotNull();
            assertThat(jsonEvent.get("warnings")).isNotNull();
            // ioMetadata is excluded via config
            assertThat(jsonEvent.get("ioMetadata")).isNull();
            assertThat(jsonEvent.get("metadata").get("queryId").textValue()).isEqualTo(TestUtils.queryCompletedEvent.getMetadata().getQueryId());

            // produce and consume a test split completed event
            eventListener.splitCompleted(TestUtils.splitCompletedEvent);
            record = getOnlyElement(pollJsonRecords(SPLIT_COMPLETED_TOPIC));
            jsonNode = MAPPER.readTree(record.value());
            jsonEvent = jsonNode.get("eventPayload");
            assertThat(jsonEvent).isNotNull();
            assertThat(jsonEvent.get("catalogName")).isNotNull();
            assertThat(jsonEvent.get("startTime")).isNotNull();
            assertThat(jsonEvent.get("endTime")).isNotNull();
            assertThat(jsonEvent.get("createTime")).isNotNull();
            assertThat(jsonEvent.get("statistics")).isNotNull();
            assertThat(jsonEvent.get("failureInfo")).isNull();
            assertThat(jsonEvent.get("payload")).isNotNull();
            assertThat(jsonEvent.get("queryId").textValue()).isEqualTo(TestUtils.splitCompletedEvent.getQueryId());
            assertThat(jsonEvent.get("stageId").textValue()).isEqualTo(TestUtils.splitCompletedEvent.getStageId());
            assertThat(jsonEvent.get("taskId").textValue()).isEqualTo(TestUtils.splitCompletedEvent.getTaskId());
        }
        finally {
            eventListener.shutdown();
        }
    }

    private ConsumerRecords<String, String> pollJsonRecords(String topic)
    {
        try (KafkaConsumer<String, String> jsonConsumer = createConsumer()) {
            jsonConsumer.subscribe(ImmutableList.of(topic));
            return jsonConsumer.poll(Duration.of(5, ChronoUnit.SECONDS));
        }
    }

    private KafkaConsumer<String, String> createConsumer()
    {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, testingKafka.getConnectString());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(properties);
    }
}
