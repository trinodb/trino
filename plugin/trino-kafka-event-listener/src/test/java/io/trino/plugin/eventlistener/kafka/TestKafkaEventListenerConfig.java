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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;

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
                .setMaxRequestSize(DataSize.of(5, MEGABYTE))
                .setBatchSize(DataSize.of(16, KILOBYTE))
                .setClientId(null)
                .setExcludedFields(Set.of())
                .setRequestTimeout(new Duration(10, TimeUnit.SECONDS))
                .setTerminateOnInitializationFailure(true)
                .setResourceConfigFiles(List.of())
                .setEnvironmentVariablePrefix(null)
                .setSaslMechanism(null)
                .setSecurityProtocol(null)
                .setSaslJaasConfig(null));
    }

    @Test
    void testExplicitPropertyMappings()
            throws IOException
    {
        Path resource1 = Files.createTempFile(null, null);
        Path resource2 = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kafka-event-listener.publish-created-event", "false")
                .put("kafka-event-listener.publish-completed-event", "false")
                .put("kafka-event-listener.publish-split-completed-event", "true")
                .put("kafka-event-listener.broker-endpoints", "kafka-host-1:9093,kafka-host-2:9093")
                .put("kafka-event-listener.max-request-size", "1048576B")
                .put("kafka-event-listener.batch-size", "81920B")
                .put("kafka-event-listener.created-event.topic", "query_created")
                .put("kafka-event-listener.completed-event.topic", "query_completed")
                .put("kafka-event-listener.split-completed-event.topic", "split_completed")
                .put("kafka-event-listener.client-id", "dashboard-cluster")
                .put("kafka-event-listener.excluded-fields", "payload,ioMetadata,groups,cpuTimeDistribution")
                .put("kafka-event-listener.request-timeout", "3s")
                .put("kafka-event-listener.env-var-prefix", "INSIGHTS_")
                .put("kafka-event-listener.anonymization.enabled", "true")
                .put("kafka-event-listener.terminate-on-initialization-failure", "false")
                .put("kafka-event-listener.config.resources", resource1.toString() + "," + resource2.toString())
                .put("kafka-event-listener.sasl-mechanism", "SCRAM-SHA-256")
                .put("kafka-event-listener.security-protocol", "SCRAM-SHA-256")
                .put("kafka-event-listener.sasl-jaas-config", "org.apache.kafka.common.security.scram.ScramLoginModule")
                .buildOrThrow();

        KafkaEventListenerConfig expected = new KafkaEventListenerConfig()
                .setAnonymizationEnabled(true)
                .setPublishCreatedEvent(false)
                .setPublishCompletedEvent(false)
                .setPublishSplitCompletedEvent(true)
                .setBrokerEndpoints("kafka-host-1:9093,kafka-host-2:9093")
                .setMaxRequestSize(DataSize.of(1048576, BYTE))
                .setBatchSize(DataSize.of(81920, BYTE))
                .setCreatedTopicName("query_created")
                .setCompletedTopicName("query_completed")
                .setSplitCompletedTopicName("split_completed")
                .setClientId("dashboard-cluster")
                .setExcludedFields(Set.of("payload", "ioMetadata", "groups", "cpuTimeDistribution"))
                .setRequestTimeout(new Duration(3, TimeUnit.SECONDS))
                .setEnvironmentVariablePrefix("INSIGHTS_")
                .setResourceConfigFiles(ImmutableList.of(resource1.toString(), resource2.toString()))
                .setTerminateOnInitializationFailure(false)
                .setSaslMechanism("SCRAM-SHA-256")
                .setSecurityProtocol("SCRAM-SHA-256")
                .setSaslJaasConfig("org.apache.kafka.common.security.scram.ScramLoginModule");

        assertFullMapping(properties, expected);
    }

    @Test
    void testExcludedFields()
    {
        KafkaEventListenerConfig conf = new KafkaEventListenerConfig();
        // check default
        Set<String> excludedFields = conf.getExcludedFields();
        assertThat(excludedFields).isEmpty();

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
}
