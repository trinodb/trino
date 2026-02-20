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
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.MountableFile;

import java.util.Map;

public class KafkaSslEnvironment
        extends KafkaBasicEnvironment
{
    @Override
    protected TestingKafka createKafka()
    {
        return TestingKafka.createSsl();
    }

    @Override
    protected Map<String, String> getKafkaCatalogProperties()
    {
        Map<String, String> properties = super.getKafkaCatalogProperties();
        properties.put("kafka.nodes", "kafka:9093");
        properties.put("kafka.security-protocol", "SSL");
        properties.put("kafka.ssl.keystore.type", "PKCS12");
        properties.put("kafka.ssl.keystore.location", "/etc/trino/catalog/secrets/kafka.client.keystore");
        properties.put("kafka.ssl.keystore.password", "confluent");
        properties.put("kafka.ssl.truststore.type", "PKCS12");
        properties.put("kafka.ssl.truststore.location", "/etc/trino/catalog/secrets/kafka.client.truststore");
        properties.put("kafka.ssl.truststore.password", "confluent");
        properties.put("kafka.ssl.key.password", "confluent");
        properties.put("kafka.ssl.endpoint-identification-algorithm", "https");
        return properties;
    }

    @Override
    protected void customizeTrinoContainer(TrinoContainer container)
    {
        container.withCopyFileToContainer(
                MountableFile.forClasspathResource("kafka/ssl/secrets/kafka.client.keystore"),
                "/etc/trino/catalog/secrets/kafka.client.keystore");
        container.withCopyFileToContainer(
                MountableFile.forClasspathResource("kafka/ssl/secrets/kafka.client.truststore"),
                "/etc/trino/catalog/secrets/kafka.client.truststore");
    }
}
