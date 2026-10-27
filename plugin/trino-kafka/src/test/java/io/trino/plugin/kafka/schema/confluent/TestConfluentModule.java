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
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestConfluentModule
{
    private static final ConfluentSchemaRegistryConfig REGISTRY_CONFIG = new ConfluentSchemaRegistryConfig()
            .setConfluentSchemaRegistryUrls(ImmutableSet.of("http://localhost:8081"));

    @Test
    public void testClientPropertiesOfAllProvidersAreCombined()
            throws IOException
    {
        try (SchemaRegistryClient client = ConfluentModule.createSchemaRegistryClient(
                REGISTRY_CONFIG,
                ImmutableSet.of(new AvroSchemaProvider()),
                ImmutableSet.of(new SchemaRegistryClientTtlProvider(REGISTRY_CONFIG), new SchemaRegistryClientSecurityProvider(new ConfluentSchemaRegistrySecurityConfig())),
                getClass().getClassLoader())) {
            assertThat(client).isNotNull();
        }
    }

    @Test
    public void testClientPropertySetByTwoProvidersIsRejected()
    {
        // an embedder's provider may not override a property the connector configures; this fails
        // at startup with the property and both providers named instead of a duplicate key error
        SchemaRegistryClientPropertiesProvider embedderProvider = () -> ImmutableMap.of("schema.registry.ssl.endpoint.identification.algorithm", "");
        assertThatThrownBy(() -> ConfluentModule.createSchemaRegistryClient(
                REGISTRY_CONFIG,
                ImmutableSet.of(new AvroSchemaProvider()),
                ImmutableSet.of(new SchemaRegistryClientSecurityProvider(new ConfluentSchemaRegistrySecurityConfig()), embedderProvider),
                getClass().getClassLoader()))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Schema registry client property 'schema.registry.ssl.endpoint.identification.algorithm' is set by both %s and %s"
                        .formatted(SchemaRegistryClientSecurityProvider.class.getName(), embedderProvider.getClass().getName()))
                .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(CONFIGURATION_INVALID.toErrorCode()));
    }

    @Test
    public void testClientPropertyWithoutValueIsRejected()
    {
        Map<String, Object> properties = new HashMap<>();
        properties.put("schema.registry.ssl.truststore.location", null);
        SchemaRegistryClientPropertiesProvider embedderProvider = () -> properties;
        assertThatThrownBy(() -> ConfluentModule.createSchemaRegistryClient(
                REGISTRY_CONFIG,
                ImmutableSet.of(new AvroSchemaProvider()),
                ImmutableSet.of(embedderProvider),
                getClass().getClassLoader()))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Schema registry client property 'schema.registry.ssl.truststore.location' set by %s has no value".formatted(embedderProvider.getClass().getName()))
                .satisfies(e -> assertThat(((TrinoException) e).getErrorCode()).isEqualTo(CONFIGURATION_INVALID.toErrorCode()));
    }
}
