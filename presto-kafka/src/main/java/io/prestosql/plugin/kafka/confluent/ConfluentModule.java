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
package io.prestosql.plugin.kafka.confluent;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.prestosql.decoder.avro.AvroReaderSupplierFactory;

import javax.inject.Singleton;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class ConfluentModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(ConfluentSchemaRegistryConfig.class);
        newMapBinder(binder, String.class, AvroReaderSupplierFactory.class).addBinding(ConfluentAvroReaderSupplier.NAME).to(ConfluentAvroReaderSupplier.Factory.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public SchemaRegistryClient getSchemaRegistryClient(ConfluentSchemaRegistryConfig confluentConfig)
    {
        return new CachedSchemaRegistryClient(confluentConfig.getConfluentSchemaRegistryUrl(), confluentConfig.getConfluentSchemaRegistryClientCacheSize());
    }
}
