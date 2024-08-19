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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestKafkaEventListenerConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(KafkaEventListenerConfig.class)
                .setAnonymizationEnabled(false)
                .setPublishCreatedEvent(true)
                .setPublishCompletedEvent(true)
                .setPublishSplitCompletedEvent(false)
                .setCompletedTopicName(null)
                .setCreatedTopicName(null)
                .setSplitCompletedTopicName(null)
                .setBrokerEndpoints(null)
                .setClientId(null)
                .setExcludedFields(Set.of())
                .setKafkaClientOverrides("")
                .setRequestTimeout(new Duration(10, TimeUnit.SECONDS))
                .setTerminateOnInitializationFailure(true)
                .setEnvironmentVariablePrefix(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kafka-event-listener.publish-created-event", "false")
                .put("kafka-event-listener.publish-completed-event", "false")
                .put("kafka-event-listener.publish-split-completed-event", "true")
                .put("kafka-event-listener.broker-endpoints", "kafka-host-1:9093,kafka-host-2:9093")
                .put("kafka-event-listener.created-event.topic", "query_created")
                .put("kafka-event-listener.completed-event.topic", "query_completed")
                .put("kafka-event-listener.split-completed-event.topic", "split_completed")
                .put("kafka-event-listener.client-id", "dashboard-cluster")
                .put("kafka-event-listener.excluded-fields", "payload,ioMetadata,groups,cpuTimeDistribution")
                .put("kafka-event-listener.client-config-overrides", "foo=bar,baz=yoo")
                .put("kafka-event-listener.request-timeout", "3s")
                .put("kafka-event-listener.env-var-prefix", "INSIGHTS_")
                .put("kafka-event-listener.anonymization.enabled", "true")
                .put("kafka-event-listener.terminate-on-initialization-failure", "false")
                .buildOrThrow();

        KafkaEventListenerConfig expected = new KafkaEventListenerConfig()
                .setAnonymizationEnabled(true)
                .setPublishCreatedEvent(false)
                .setPublishCompletedEvent(false)
                .setPublishSplitCompletedEvent(true)
                .setBrokerEndpoints("kafka-host-1:9093,kafka-host-2:9093")
                .setCreatedTopicName("query_created")
                .setCompletedTopicName("query_completed")
                .setSplitCompletedTopicName("split_completed")
                .setClientId("dashboard-cluster")
                .setExcludedFields(Set.of("payload", "ioMetadata", "groups", "cpuTimeDistribution"))
                .setKafkaClientOverrides("foo=bar,baz=yoo")
                .setRequestTimeout(new Duration(3, TimeUnit.SECONDS))
                .setEnvironmentVariablePrefix("INSIGHTS_")
                .setTerminateOnInitializationFailure(false);

        assertFullMapping(properties, expected);
    }

    @Test
    void testExcludedFields()
    {
        KafkaEventListenerConfig conf = new KafkaEventListenerConfig();
        // check default
        Set<String> excludedFields = conf.getExcludedFields();
        assertThat(excludedFields.size()).isEqualTo(0);

        // check setting multiple
        conf.setExcludedFields(Set.of("payload", "plan", "user", "groups"));
        excludedFields = conf.getExcludedFields();
        assertThat(excludedFields)
                .containsOnly("payload", "plan", "user", "groups");

        // setting to empty
        conf.setExcludedFields(Set.of(""));
        excludedFields = conf.getExcludedFields();
        assertThat(excludedFields).isEmpty();

        // setting to empty with commas
        conf.setExcludedFields(Set.of(" ", ""));
        excludedFields = conf.getExcludedFields();
        assertThat(excludedFields).isEmpty();
    }

    @Test
    void testKafkaClientOverrides()
    {
        KafkaEventListenerConfig conf = new KafkaEventListenerConfig();
        // check default
        Map<String, String> overrides = conf.getKafkaClientOverrides();
        assertThat(overrides).isEmpty();

        // check setting just one
        conf.setKafkaClientOverrides("buffer.memory=444555");
        overrides = conf.getKafkaClientOverrides();
        assertThat(overrides).containsExactly(entry("buffer.memory", "444555"));

        // check setting multiple
        conf.setKafkaClientOverrides("buffer.memory=444555, compression.type=zstd");
        overrides = conf.getKafkaClientOverrides();
        assertThat(overrides)
                .containsExactly(entry("buffer.memory", "444555"), entry("compression.type", "zstd"));

        // check empty trailing param
        conf.setKafkaClientOverrides("buffer.memory=555777,");
        overrides = conf.getKafkaClientOverrides();
        assertThat(overrides).containsExactly(entry("buffer.memory", "555777"));

        conf.setKafkaClientOverrides(",, ,");
        overrides = conf.getKafkaClientOverrides();
        assertThat(overrides).isEmpty();

        // check missing = throws
        assertThatThrownBy(() -> conf.setKafkaClientOverrides("invalid,buffer.memory=555777"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
