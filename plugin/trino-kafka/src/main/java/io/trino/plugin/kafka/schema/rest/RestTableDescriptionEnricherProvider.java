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

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.trino.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

/**
 * Provider for RestTableDescriptionEnricher to avoid Guice recursive binding
 * when using OptionalBinder.
 */
public class RestTableDescriptionEnricherProvider
        implements Provider<RestTableDescriptionEnricher>
{
    private final SchemaRegistryClient schemaRegistryClient;
    private final TypeManager typeManager;

    @Inject
    public RestTableDescriptionEnricherProvider(
            SchemaRegistryClient schemaRegistryClient,
            TypeManager typeManager)
    {
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public RestTableDescriptionEnricher get()
    {
        return new RestTableDescriptionEnricher(schemaRegistryClient, typeManager);
    }
}
