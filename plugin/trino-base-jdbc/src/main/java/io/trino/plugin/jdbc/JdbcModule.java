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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.trino.plugin.base.CatalogName;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.procedure.Procedure;

import javax.inject.Provider;
import javax.inject.Singleton;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class JdbcModule
        implements Module
{
    private final String catalogName;

    public JdbcModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
        binder.install(new JdbcDiagnosticModule());

        newOptionalBinder(binder, ConnectorAccessControl.class);

        procedureBinder(binder);
        tablePropertiesProviderBinder(binder);

        binder.bind(JdbcMetadataFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorSplitManager.class).setDefault().to(JdbcSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorRecordSetProvider.class).setDefault().to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorPageSinkProvider.class).setDefault().to(JdbcPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(JdbcMetadataConfig.class);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);

        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        bindSessionPropertiesProvider(binder, TypeHandlingJdbcSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcMetadataSessionProperties.class);

        binder.bind(JdbcClient.class).to(CachingJdbcClient.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class));
    }

    @Provides
    @Singleton
    public ConnectionFactory createConnectionFactory(@StatsCollecting ConnectionFactory connectionFactory)
    {
        return new LazyConnectionFactory(connectionFactory);
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
}
