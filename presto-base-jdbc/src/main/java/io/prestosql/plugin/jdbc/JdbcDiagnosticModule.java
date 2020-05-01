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
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.plugin.base.util.LoggingInvocationHandler;
import io.prestosql.plugin.base.util.LoggingInvocationHandler.ReflectiveParameterNamesProvider;
import io.prestosql.plugin.jdbc.jmx.StatisticsAwareConnectionFactory;
import io.prestosql.plugin.jdbc.jmx.StatisticsAwareJdbcClient;
import org.weakref.jmx.guice.MBeanModule;

import static com.google.common.reflect.Reflection.newProxy;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class JdbcDiagnosticModule
        implements Module
{
    private final String catalogName;

    public JdbcDiagnosticModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.install(new MBeanServerModule());
        binder.install(new MBeanModule());

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
        Logger logger = Logger.get(format("io.prestosql.plugin.jdbc.%s.jdbcclient", catalogName));

        JdbcClient loggingInvocationsJdbcClient = newProxy(JdbcClient.class, new LoggingInvocationHandler(
                client,
                new ReflectiveParameterNamesProvider(),
                logger::debug));

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
