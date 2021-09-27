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
package io.trino.delta;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.DynamicConfigurationProvider;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.concurrent.ExecutorService;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DeltaModule
        implements Module
{
    private final String connectorId;
    private final TypeManager typeManager;

    public DeltaModule(String connectorId, TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(DeltaConnector.class).in(Scopes.SINGLETON);
        binder.bind(DeltaConnectorId.class).toInstance(new DeltaConnectorId(connectorId));
        binder.bind(DeltaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DeltaClient.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DeltaPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSessionProperties.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DeltaConfig.class);

        configBinder(binder).bindConfig(MetastoreConfig.class);
        configBinder(binder).bindConfig(HiveConfig.class);

        binder.bind(HdfsConfigurationInitializer.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).to(HiveHdfsConfiguration.class).in(Scopes.SINGLETON);
        newSetBinder(binder, DynamicConfigurationProvider.class);
        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(DeltaTable.class));

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).as(generatedNameOf(FileFormatDataSourceStats.class, connectorId));
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "metadata is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(TypeId.of(value));
        }
    }

    @Singleton
    @Provides
    public ExecutorService createDeltaClientExecutor(CatalogName catalogName)
    {
        return newCachedThreadPool(daemonThreadsNamed("delta-" + catalogName + "-%s"));
    }
}
