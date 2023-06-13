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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.MapBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.trino.decoder.DispatchingRowDecoderFactory;
import io.trino.decoder.RowDecoderFactory;
import io.trino.decoder.avro.AvroBytesDeserializer;
import io.trino.decoder.avro.AvroDeserializer;
import io.trino.decoder.avro.AvroReaderSupplier;
import io.trino.decoder.avro.AvroRowDecoderFactory;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.decoder.dummy.DummyRowDecoderFactory;
import io.trino.decoder.protobuf.DynamicMessageProvider;
import io.trino.decoder.protobuf.ProtobufRowDecoder;
import io.trino.decoder.protobuf.ProtobufRowDecoderFactory;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.kafka.encoder.DispatchingRowEncoderFactory;
import io.trino.plugin.kafka.encoder.RowEncoderFactory;
import io.trino.plugin.kafka.encoder.avro.AvroRowEncoder;
import io.trino.plugin.kafka.encoder.protobuf.ProtobufRowEncoder;
import io.trino.plugin.kafka.encoder.protobuf.ProtobufSchemaParser;
import io.trino.plugin.kafka.schema.ContentSchemaReader;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.kafka.encoder.EncoderModule.encoderFactory;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class ConfluentModule
        extends AbstractConfigurationAwareModule
{
    private final TypeManager typeManager;

    public ConfluentModule(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);

        configBinder(binder).bindConfig(ConfluentSchemaRegistryConfig.class);
        install(new ConfluentDecoderModule());
        install(new ConfluentEncoderModule());
        binder.bind(ContentSchemaReader.class).to(AvroConfluentContentSchemaReader.class).in(Scopes.SINGLETON);
        newSetBinder(binder, SchemaRegistryClientPropertiesProvider.class);
        newSetBinder(binder, SchemaProvider.class).addBinding().to(AvroSchemaProvider.class).in(Scopes.SINGLETON);
        // Each SchemaRegistry object should have a new instance of SchemaProvider
        newSetBinder(binder, SchemaProvider.class).addBinding().to(LazyLoadedProtobufSchemaProvider.class);
        binder.bind(DynamicMessageProvider.Factory.class).to(ConfluentSchemaRegistryDynamicMessageProvider.Factory.class).in(SINGLETON);
        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(ConfluentSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(TableDescriptionSupplier.class).toProvider(ConfluentSchemaRegistryTableDescriptionSupplier.Factory.class).in(Scopes.SINGLETON);
        newMapBinder(binder, String.class, SchemaParser.class).addBinding("AVRO").to(AvroSchemaParser.class).in(Scopes.SINGLETON);
        newMapBinder(binder, String.class, SchemaParser.class).addBinding("PROTOBUF").to(LazyLoadedProtobufSchemaParser.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static SchemaRegistryClient createSchemaRegistryClient(
            ConfluentSchemaRegistryConfig confluentConfig,
            Set<SchemaProvider> schemaProviders,
            Set<SchemaRegistryClientPropertiesProvider> propertiesProviders,
            ClassLoader classLoader)
    {
        requireNonNull(schemaProviders, "schemaProviders is null");
        requireNonNull(propertiesProviders, "propertiesProviders is null");

        List<String> baseUrl = confluentConfig.getConfluentSchemaRegistryUrls().stream()
                .map(HostAddress::getHostText)
                .collect(toImmutableList());

        Map<String, ?> schemaRegistryClientProperties = propertiesProviders.stream()
                .map(SchemaRegistryClientPropertiesProvider::getSchemaRegistryClientProperties)
                .flatMap(properties -> properties.entrySet().stream())
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        return new ClassLoaderSafeSchemaRegistryClient(
                new CachedSchemaRegistryClient(
                        baseUrl,
                        confluentConfig.getConfluentSchemaRegistryClientCacheSize(),
                        ImmutableList.copyOf(schemaProviders),
                        schemaRegistryClientProperties),
                classLoader);
    }

    private static class ConfluentDecoderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(AvroReaderSupplier.Factory.class).to(ConfluentAvroReaderSupplier.Factory.class).in(Scopes.SINGLETON);
            binder.bind(AvroDeserializer.Factory.class).to(AvroBytesDeserializer.Factory.class).in(Scopes.SINGLETON);
            newMapBinder(binder, String.class, RowDecoderFactory.class).addBinding(AvroRowDecoderFactory.NAME).to(AvroRowDecoderFactory.class).in(Scopes.SINGLETON);
            newMapBinder(binder, String.class, RowDecoderFactory.class).addBinding(ProtobufRowDecoder.NAME).to(ProtobufRowDecoderFactory.class).in(Scopes.SINGLETON);
            newMapBinder(binder, String.class, RowDecoderFactory.class).addBinding(DummyRowDecoder.NAME).to(DummyRowDecoderFactory.class).in(SINGLETON);
            binder.bind(DispatchingRowDecoderFactory.class).in(SINGLETON);
        }
    }

    private static class ConfluentEncoderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            MapBinder<String, RowEncoderFactory> encoderFactoriesByName = encoderFactory(binder);
            encoderFactoriesByName.addBinding(AvroRowEncoder.NAME).toInstance((session, dataSchema, columnHandles) -> {
                throw new TrinoException(NOT_SUPPORTED, "Insert not supported");
            });
            encoderFactoriesByName.addBinding(ProtobufRowEncoder.NAME).toInstance((session, dataSchema, columnHandles) -> {
                throw new TrinoException(NOT_SUPPORTED, "Insert is not supported for schema registry based tables");
            });
            binder.bind(DispatchingRowEncoderFactory.class).in(SINGLETON);
        }
    }

    private static class LazyLoadedProtobufSchemaProvider
            implements SchemaProvider
    {
        // Make JVM to load lazily ProtobufSchemaProvider, so Kafka connector can be used
        // without protobuf dependency for non protobuf based topics
        private final Supplier<SchemaProvider> delegate = Suppliers.memoize(this::create);
        private final AtomicReference<Map<String, ?>> configuration = new AtomicReference<>();

        @Override
        public String schemaType()
        {
            return "PROTOBUF";
        }

        @Override
        public void configure(Map<String, ?> configuration)
        {
            Map<String, ?> oldConfiguration = this.configuration.getAndSet(ImmutableMap.copyOf(configuration));
            checkState(oldConfiguration == null, "ProtobufSchemaProvider is already configured");
        }

        @Override
        public Optional<ParsedSchema> parseSchema(String schema, List<SchemaReference> references, boolean isNew)
        {
            return delegate.get().parseSchema(schema, references, isNew);
        }

        @Override
        public ParsedSchema parseSchemaOrElseThrow(Schema schema, boolean isNew)
        {
            return delegate.get().parseSchemaOrElseThrow(schema, isNew);
        }

        private SchemaProvider create()
        {
            ProtobufSchemaProvider schemaProvider = new ProtobufSchemaProvider();
            Map<String, ?> configuration = this.configuration.get();
            checkState(configuration != null, "ProtobufSchemaProvider is not already configured");
            schemaProvider.configure(configuration);
            return schemaProvider;
        }
    }

    public static class LazyLoadedProtobufSchemaParser
            extends ForwardingSchemaParser
    {
        // Make JVM to load lazily ProtobufSchemaParser, so Kafka connector can be used
        // without protobuf dependency for non protobuf based topics
        private final Supplier<SchemaParser> delegate;

        @Inject
        public LazyLoadedProtobufSchemaParser(TypeManager typeManager)
        {
            this.delegate = Suppliers.memoize(() -> new ProtobufSchemaParser(requireNonNull(typeManager, "typeManager is null")));
        }

        @Override
        protected SchemaParser delegate()
        {
            return delegate.get();
        }
    }
}
