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
package io.prestosql.plugin.jdbc;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.prestosql.plugin.base.util.LoggingInvocationHandler;
import io.prestosql.plugin.base.util.LoggingInvocationHandler.ReflectiveParameterNamesProvider;
import io.prestosql.plugin.jdbc.jmx.StatisticsAwareConnectionFactory;
import io.prestosql.plugin.jdbc.jmx.StatisticsAwareJdbcClient;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.procedure.Procedure;

import static com.google.common.reflect.Reflection.newProxy;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

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
        newOptionalBinder(binder, ConnectorAccessControl.class);
        newSetBinder(binder, Procedure.class);
        newSetBinder(binder, SessionPropertiesProvider.class);
        binder.bind(JdbcMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(JdbcMetadataConfig.class);

        binder.bind(JdbcClient.class)
                .to(Key.get(JdbcClient.class, StatsCollecting.class));
        binder.bind(ConnectionFactory.class)
                .to(Key.get(ConnectionFactory.class, StatsCollecting.class));

        newExporter(binder).export(Key.get(JdbcClient.class, StatsCollecting.class))
                .as(generator -> generator.generatedNameOf(JdbcClient.class, catalogName));
        newExporter(binder).export(Key.get(ConnectionFactory.class, StatsCollecting.class))
                .as(generator -> generator.generatedNameOf(ConnectionFactory.class, catalogName));
    }

    @Provides
    @Singleton
    @StatsCollecting
    public JdbcClient createJdbcClientWithStats(@ForBaseJdbc JdbcClient client)
    {
        StatisticsAwareJdbcClient statisticsAwareJdbcClient = new StatisticsAwareJdbcClient(client);

        Logger logger = Logger.get(format("io.prestosql.plugin.jdbc.%s.jdbcclient", catalogName));

        JdbcClient loggingInvocationsJdbcClient = newProxy(JdbcClient.class, new LoggingInvocationHandler(
                statisticsAwareJdbcClient,
                new ReflectiveParameterNamesProvider(),
                logger::debug));

        return new ForwardingJdbcClient()
        {
            @Override
            protected JdbcClient getDelegate()
            {
                if (logger.isDebugEnabled()) {
                    return loggingInvocationsJdbcClient;
                }
                return statisticsAwareJdbcClient;
            }
        };
    }

    @Provides
    @Singleton
    @StatsCollecting
    public static ConnectionFactory createConnectionFactoryWithStats(@ForBaseJdbc ConnectionFactory connectionFactory)
    {
        return new StatisticsAwareConnectionFactory(connectionFactory);
    }
}
