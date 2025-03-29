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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.trino.plugin.kafka.KafkaQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public final class TestKafkaWithConfluentSchemaRegistryBasicAuth
        extends TestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.createWithSchemaRegistry(true));
        testingKafka.start();
        return KafkaQueryRunner.builderForConfluentSchemaRegistry(testingKafka)
                .addConnectorProperties(ImmutableMap.of(
                        "kafka.confluent-subjects-cache-refresh-interval", "1ms",
                        "kafka.confluent-schema-registry.basic-auth.username", "user",
                        "kafka.confluent-schema-registry.basic-auth.password", "31337",
                        "kafka.confluent-schema-registry-auth-type", "BASIC_AUTH"))
                .build();
    }

    @Test
    public void testBasicTopicBasicAuth()
    {
        String topic = "topic-basic-MixedCase-" + randomNameSuffix();
        assertTopic(
                topic,
                format("SELECT col_1, col_2 FROM %s", toDoubleQuoted(topic)),
                format("SELECT col_1, col_2, col_3 FROM %s", toDoubleQuoted(topic)),
                false,
                schemaRegistryAwareProducerBasicAuth(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .buildOrThrow());
    }

    private static ImmutableMap.Builder<String, String> schemaRegistryAwareProducerBasicAuth(TestingKafka testingKafka)
    {
        return ImmutableMap.<String, String>builder()
                .put(SCHEMA_REGISTRY_URL_CONFIG, testingKafka.getSchemaRegistryConnectString())
                .put(BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO")
                .put(USER_INFO_CONFIG, "user:31337");
    }
}
