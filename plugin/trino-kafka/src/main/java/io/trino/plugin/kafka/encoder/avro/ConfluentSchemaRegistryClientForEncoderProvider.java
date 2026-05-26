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
package io.trino.plugin.kafka.encoder.avro;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.trino.plugin.kafka.schema.confluent.ConfluentSchemaRegistryConfig;
import io.trino.spi.HostAddress;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Provides a Confluent Schema Registry client for Avro encoding when
 * {@code kafka.confluent-schema-registry-url} is set on the REST table description supplier.
 * Used to resolve schemas and decode Confluent wire-format Avro messages
 * (magic byte + schema ID + payload).
 */
public class ConfluentSchemaRegistryClientForEncoderProvider
        implements Provider<SchemaRegistryClient>
{
    private static final int DEFAULT_SCHEMA_REGISTRY_PORT = 8081;

    private final ConfluentSchemaRegistryConfig config;

    @Inject
    public ConfluentSchemaRegistryClientForEncoderProvider(ConfluentSchemaRegistryConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public SchemaRegistryClient get()
    {
        List<String> urls = config.getConfluentSchemaRegistryUrlStrings().isEmpty()
                ? config.getConfluentSchemaRegistryUrls().stream()
                  .map(ConfluentSchemaRegistryClientForEncoderProvider::hostAddressToUrlString)
                  .collect(toImmutableList())
                : config.getConfluentSchemaRegistryUrlStrings();

        return new CachedSchemaRegistryClient(
                urls,
                config.getConfluentSchemaRegistryClientCacheSize(),
                List.of(new AvroSchemaProvider()),
                null);
    }

    private static String hostAddressToUrlString(HostAddress address)
    {
        int port = address.hasPort() ? address.getPort() : DEFAULT_SCHEMA_REGISTRY_PORT;
        return "http://" + address.getHostText() + ":" + port;
    }
}
