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
package io.prestosql.plugin.kafka.schema.confluent;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.prestosql.decoder.avro.AvroBytesDeserializer;
import io.prestosql.decoder.avro.AvroDeserializer;
import io.prestosql.decoder.avro.AvroReaderSupplier;
import io.prestosql.plugin.kafka.schema.ContentSchemaReader;

import javax.inject.Singleton;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ConfluentModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(ConfluentSchemaRegistryConfig.class);
        binder.bind(AvroReaderSupplier.Factory.class).to(ConfluentAvroReaderSupplier.Factory.class).in(Scopes.SINGLETON);
        binder.bind(AvroDeserializer.Factory.class).to(AvroBytesDeserializer.Factory.class).in(Scopes.SINGLETON);
        binder.bind(ContentSchemaReader.class).to(AvroConfluentContentSchemaReader.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public SchemaRegistryClient getSchemaRegistryClient(ConfluentSchemaRegistryConfig confluentConfig)
    {
        return new CachedSchemaRegistryClient(confluentConfig.getConfluentSchemaRegistryUrl(), confluentConfig.getConfluentSchemaRegistryClientCacheSize());
    }
}
