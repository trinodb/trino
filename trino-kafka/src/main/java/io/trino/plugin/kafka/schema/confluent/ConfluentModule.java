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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.prestosql.decoder.DecoderModule;
import io.prestosql.decoder.RowDecoderFactory;
import io.prestosql.decoder.avro.AvroBytesDeserializer;
import io.prestosql.decoder.avro.AvroDeserializer;
import io.prestosql.decoder.avro.AvroReaderSupplier;
import io.prestosql.decoder.avro.AvroRowDecoderFactory;
import io.prestosql.plugin.kafka.SessionPropertiesProvider;
import io.prestosql.plugin.kafka.encoder.EncoderModule;
import io.prestosql.plugin.kafka.encoder.RowEncoderFactory;
import io.prestosql.plugin.kafka.encoder.avro.AvroRowEncoder;
import io.prestosql.plugin.kafka.schema.ContentSchemaReader;
import io.prestosql.plugin.kafka.schema.TableDescriptionSupplier;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;

import javax.inject.Singleton;

import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.kafka.encoder.EncoderModule.encoderFactory;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;

public class ConfluentModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(ConfluentSchemaRegistryConfig.class);
        install(new DecoderModule(new ConfluentAvroDecoderModule()));
        install(new EncoderModule(new ConfluentAvroEncoderModule()));
        binder.bind(ContentSchemaReader.class).to(AvroConfluentContentSchemaReader.class).in(Scopes.SINGLETON);
        newSetBinder(binder, SchemaProvider.class).addBinding().to(AvroSchemaProvider.class).in(Scopes.SINGLETON);
        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(ConfluentSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(TableDescriptionSupplier.class).toProvider(ConfluentSchemaRegistryTableDescriptionSupplier.Factory.class).in(Scopes.SINGLETON);
        newMapBinder(binder, String.class, SchemaParser.class).addBinding("AVRO").to(AvroSchemaParser.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static SchemaRegistryClient getSchemaRegistryClient(ConfluentSchemaRegistryConfig confluentConfig, Set<SchemaProvider> schemaProviders)
    {
        return new CachedSchemaRegistryClient(
                confluentConfig.getConfluentSchemaRegistryUrls().stream()
                        .map(HostAddress::getHostText)
                        .collect(toImmutableList()),
                confluentConfig.getConfluentSchemaRegistryClientCacheSize(),
                ImmutableList.copyOf(schemaProviders),
                ImmutableMap.of());
    }

    private static class ConfluentAvroDecoderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(AvroReaderSupplier.Factory.class).to(ConfluentAvroReaderSupplier.Factory.class).in(Scopes.SINGLETON);
            binder.bind(AvroDeserializer.Factory.class).to(AvroBytesDeserializer.Factory.class).in(Scopes.SINGLETON);
            newMapBinder(binder, String.class, RowDecoderFactory.class).addBinding(AvroRowDecoderFactory.NAME).to(AvroRowDecoderFactory.class).in(Scopes.SINGLETON);
        }
    }

    private static class ConfluentAvroEncoderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            MapBinder<String, RowEncoderFactory> encoderFactoriesByName = encoderFactory(binder);
            encoderFactoriesByName.addBinding(AvroRowEncoder.NAME).toInstance((session, dataSchema, columnHandles) -> {
                throw new PrestoException(NOT_SUPPORTED, "Insert not supported");
            });
        }
    }
}
