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
package io.trino.plugin.kafka.schema.rest;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.OptionalBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.decoder.protobuf.DescriptorProvider;
import io.trino.decoder.protobuf.FileDescriptorProvider;
import io.trino.plugin.kafka.encoder.EncoderModule;
import io.trino.plugin.kafka.encoder.avro.ConfluentSchemaRegistryClientForEncoderProvider;
import io.trino.plugin.kafka.schema.ContentSchemaProvider;
import io.trino.plugin.kafka.schema.ProtobufAnySupportConfig;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.plugin.kafka.schema.confluent.ConfluentSchemaRegistryConfig;
import io.trino.plugin.kafka.schema.file.FileReadContentSchemaProvider;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Table description supplier that reads table and schema definitions from a REST API
 * instead of the file system. The API must return a JSON array of KafkaTopicDescription objects.
 * Schema is resolved from file or URL (dataSchema) in each table description.
 * When Schema Registry is configured, "fields" may be omitted for Avro key/message; they are
 * derived from the registry by subject (or topic-key / topic-value).
 */
public class RestTableDescriptionSupplierModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(RestTableDescriptionSupplierConfig.class);

        boolean schemaRegistryConfigured = !buildConfigObject(RestTableDescriptionSupplierConfig.class)
                .getConfluentSchemaRegistryUrls().isEmpty();

        OptionalBinder<io.confluent.kafka.schemaregistry.client.SchemaRegistryClient> schemaRegistryBinder =
                OptionalBinder.newOptionalBinder(binder, io.confluent.kafka.schemaregistry.client.SchemaRegistryClient.class);
        OptionalBinder<RestTableDescriptionEnricher> enricherBinder =
                OptionalBinder.newOptionalBinder(binder, RestTableDescriptionEnricher.class);

        if (schemaRegistryConfigured) {
            configBinder(binder).bindConfig(ConfluentSchemaRegistryConfig.class);
            schemaRegistryBinder.setBinding().toProvider(ConfluentSchemaRegistryClientForEncoderProvider.class).in(SINGLETON);
            enricherBinder.setBinding().toProvider(RestTableDescriptionEnricherProvider.class).in(SINGLETON);
        }

        binder.bind(TableDescriptionSupplier.class).to(RestTableDescriptionSupplier.class).in(SINGLETON);

        if (schemaRegistryConfigured) {
            install(new RestConfluentDecoderModule());
            binder.bind(ContentSchemaProvider.class).to(RestContentSchemaProvider.class).in(SINGLETON);
        }
        else {
            install(new RestPlainAvroDecoderModule());
            binder.bind(ContentSchemaProvider.class).to(FileReadContentSchemaProvider.class).in(SINGLETON);
        }

        install(new EncoderModule());

        configBinder(binder).bindConfig(ProtobufAnySupportConfig.class);
        install(conditionalModule(ProtobufAnySupportConfig.class,
                ProtobufAnySupportConfig::isProtobufAnySupportEnabled,
                new FileDescriptorProviderModule()));
    }

    private static class FileDescriptorProviderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(DescriptorProvider.class).to(FileDescriptorProvider.class).in(SINGLETON);
        }
    }
}
