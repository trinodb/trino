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
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.prestosql.decoder.RowDecoderFactory;
import io.prestosql.decoder.avro.AvroBytesDeserializer;
import io.prestosql.decoder.avro.AvroDeserializer;
import io.prestosql.decoder.avro.AvroReaderSupplier;
import io.prestosql.decoder.avro.AvroRowDecoderFactory;
import io.prestosql.plugin.kafka.ForInternalKeyField;
import io.prestosql.plugin.kafka.ForInternalMessageField;
import io.prestosql.plugin.kafka.ForKafka;
import io.prestosql.plugin.kafka.KafkaInternalFieldManager;
import io.prestosql.plugin.kafka.schema.ContentSchemaReader;
import io.prestosql.plugin.kafka.schema.TableDescriptionSupplier;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Singleton;

import java.util.List;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
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
        newSetBinder(binder, new TypeLiteral<List<PropertyMetadata<?>>>() {}, ForKafka.class).addBinding().toProvider(ConfluentSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(TableDescriptionSupplier.class).toProvider(ConfluentSchemaRegistryTableDescriptionSupplier.Factory.class).in(Scopes.SINGLETON);
        newMapBinder(binder, String.class, RowDecoderFactory.class).addBinding(AvroRowDecoderFactory.NAME).to(AvroRowDecoderFactory.class).in(Scopes.SINGLETON);
        newSetBinder(binder, new TypeLiteral<List<KafkaInternalFieldManager.InternalField>>() {}, ForKafka.class).addBinding().toProvider(ConfluentInternalFields.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(RowDecoderFactory.class, ForInternalKeyField.class)).setBinding().to(InternalKeyFieldDecoder.Factory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(RowDecoderFactory.class, ForInternalMessageField.class)).setBinding().to(InternalMessageFieldDecoder.Factory.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public SchemaRegistryClient getSchemaRegistryClient(ConfluentSchemaRegistryConfig confluentConfig)
    {
        return new CachedSchemaRegistryClient(confluentConfig.getConfluentSchemaRegistryUrl(), confluentConfig.getConfluentSchemaRegistryClientCacheSize());
    }
}
