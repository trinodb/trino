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
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.util.LoggingInvocationHandler;
import io.trino.plugin.jdbc.jmx.StatisticsAwareConnectionFactory;
import io.trino.plugin.jdbc.jmx.StatisticsAwareJdbcClient;
import org.weakref.jmx.guice.MBeanModule;

import static com.google.common.reflect.Reflection.newProxy;
import static java.lang.String.format;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class JdbcDiagnosticModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.install(new MBeanServerModule());
        binder.install(new MBeanModule());

        Provider<CatalogName> catalogName = binder.getProvider(CatalogName.class);
        newExporter(binder).export(Key.get(JdbcClient.class, StatsCollecting.class))
                .as(generator -> generator.generatedNameOf(JdbcClient.class, catalogName.get().toString()));
        newExporter(binder).export(Key.get(ConnectionFactory.class, StatsCollecting.class))
                .as(generator -> generator.generatedNameOf(ConnectionFactory.class, catalogName.get().toString()));
        newExporter(binder).export(JdbcClient.class)
                .as(generator -> generator.generatedNameOf(CachingJdbcClient.class, catalogName.get().toString()));
    }

    @Provides
    @Singleton
    @StatsCollecting
    public JdbcClient createJdbcClientWithStats(@ForBaseJdbc JdbcClient client, CatalogName catalogName)
    {
        Logger logger = Logger.get(format("io.trino.plugin.jdbc.%s.jdbcclient", catalogName));

        JdbcClient loggingInvocationsJdbcClient = newProxy(JdbcClient.class, new LoggingInvocationHandler(client, logger::debug));

        return new StatisticsAwareJdbcClient(ForwardingJdbcClient.of(() -> {
            if (logger.isDebugEnabled()) {
                return loggingInvocationsJdbcClient;
            }
            return client;
        }));
    }

    @Provides
    @Singleton
    @StatsCollecting
    public static ConnectionFactory createConnectionFactoryWithStats(@ForBaseJdbc ConnectionFactory connectionFactory)
    {
        return new StatisticsAwareConnectionFactory(connectionFactory);
    }
}
