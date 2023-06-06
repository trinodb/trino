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
package io.trino.plugin.jdbc;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.logging.RemoteQueryModifierModule;
import io.trino.plugin.jdbc.mapping.IdentifierMappingModule;
import io.trino.plugin.jdbc.procedure.FlushJdbcMetadataCacheProcedure;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;
import jakarta.annotation.PreDestroy;

import java.util.concurrent.ExecutorService;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class JdbcModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        install(new JdbcDiagnosticModule());
        install(new IdentifierMappingModule());
        install(new RemoteQueryModifierModule());

        newOptionalBinder(binder, ConnectorAccessControl.class);
        newOptionalBinder(binder, QueryBuilder.class).setDefault().to(DefaultQueryBuilder.class).in(Scopes.SINGLETON);

        procedureBinder(binder);
        tablePropertiesProviderBinder(binder);

        newOptionalBinder(binder, JdbcMetadataFactory.class).setDefault().to(DefaultJdbcMetadataFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(ConnectorSplitManager.class, ForJdbcDynamicFiltering.class)).setDefault().to(JdbcSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorSplitManager.class).setDefault().to(JdbcDynamicFilteringSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorRecordSetProvider.class).setDefault().to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorPageSinkProvider.class).setDefault().to(JdbcPageSinkProvider.class).in(Scopes.SINGLETON);

        binder.bind(JdbcTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(JdbcConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(JdbcMetadataConfig.class);
        configBinder(binder).bindConfig(JdbcWriteConfig.class);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(JdbcDynamicFilteringConfig.class);

        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        bindSessionPropertiesProvider(binder, TypeHandlingJdbcSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcMetadataSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcWriteSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcDynamicFilteringSessionProperties.class);

        binder.bind(DynamicFilteringStats.class).in(Scopes.SINGLETON);
        Provider<CatalogName> catalogName = binder.getProvider(CatalogName.class);
        newExporter(binder).export(DynamicFilteringStats.class)
                .as(generator -> generator.generatedNameOf(DynamicFilteringStats.class, catalogName.get().toString()));

        binder.bind(CachingJdbcClient.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).to(Key.get(CachingJdbcClient.class)).in(Scopes.SINGLETON);

        newSetBinder(binder, Procedure.class).addBinding().toProvider(FlushJdbcMetadataCacheProcedure.class).in(Scopes.SINGLETON);

        newSetBinder(binder, ConnectorTableFunction.class);

        binder.bind(ConnectionFactory.class)
                .annotatedWith(ForLazyConnectionFactory.class)
                .to(Key.get(ConnectionFactory.class, StatsCollecting.class))
                .in(Scopes.SINGLETON);
        install(conditionalModule(
                QueryConfig.class,
                QueryConfig::isReuseConnection,
                new ReusableConnectionFactoryModule(),
                innerBinder -> innerBinder.bind(ConnectionFactory.class).to(LazyConnectionFactory.class).in(Scopes.SINGLETON)));

        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class));

        newOptionalBinder(binder, Key.get(ExecutorService.class, ForRecordCursor.class))
                .setDefault()
                .toProvider(MoreExecutors::newDirectExecutorService)
                .in(Scopes.SINGLETON);

        newSetBinder(binder, JdbcQueryEventListener.class);
    }

    public static Multibinder<SessionPropertiesProvider> sessionPropertiesProviderBinder(Binder binder)
    {
        return newSetBinder(binder, SessionPropertiesProvider.class);
    }

    public static void bindSessionPropertiesProvider(Binder binder, Class<? extends SessionPropertiesProvider> type)
    {
        sessionPropertiesProviderBinder(binder).addBinding().to(type).in(Scopes.SINGLETON);
    }

    public static Multibinder<Procedure> procedureBinder(Binder binder)
    {
        return newSetBinder(binder, Procedure.class);
    }

    public static void bindProcedure(Binder binder, Class<? extends Provider<? extends Procedure>> type)
    {
        procedureBinder(binder).addBinding().toProvider(type).in(Scopes.SINGLETON);
    }

    public static Multibinder<TablePropertiesProvider> tablePropertiesProviderBinder(Binder binder)
    {
        return newSetBinder(binder, TablePropertiesProvider.class);
    }

    public static void bindTablePropertiesProvider(Binder binder, Class<? extends TablePropertiesProvider> type)
    {
        tablePropertiesProviderBinder(binder).addBinding().to(type).in(Scopes.SINGLETON);
    }

    @PreDestroy
    public void shutdownRecordCursorExecutor(@ForRecordCursor ExecutorService executor)
    {
        executor.shutdownNow();
    }
}
