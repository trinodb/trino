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
package io.trino.tests.product.kafka;

import io.trino.testing.kafka.TestingKafka;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;

import java.util.Map;

public class KafkaSaslPlaintextEnvironment
        extends KafkaBasicEnvironment
{
    @Override
    protected TestingKafka createKafka()
    {
        return TestingKafka.createSaslPlaintext();
    }

    @Override
    protected Map<String, String> getKafkaCatalogProperties()
    {
        Map<String, String> properties = super.getKafkaCatalogProperties();
        properties.put("kafka.nodes", "kafka:9093");
        properties.put("kafka.config.resources", "/etc/trino/kafka-configuration.properties");
        return properties;
    }

    @Override
    protected void customizeTrinoContainer(TrinoContainer container)
    {
        container.withCopyToContainer(
                Transferable.of("""
                        security.protocol=SASL_PLAINTEXT
                        sasl.mechanism=PLAIN
                        sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
                            username="admin" \
                            password="admin-secret";
                        """),
                "/etc/trino/kafka-configuration.properties");
    }
}
