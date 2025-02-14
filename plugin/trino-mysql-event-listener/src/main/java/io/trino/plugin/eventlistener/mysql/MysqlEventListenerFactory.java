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
package io.trino.plugin.eventlistener.mysql;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.mysql.cj.jdbc.Driver;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.spi.TrinoWarning;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.eventlistener.QueryOutputMetadata;
import org.jdbi.v3.core.ConnectionFactory;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.opentelemetry.JdbiOpenTelemetryPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class MysqlEventListenerFactory
        implements EventListenerFactory
{
    @Override
    public String getName()
    {
        return "mysql";
    }

    @Override
    public EventListener create(Map<String, String> config, EventListenerContext context)
    {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new MysqlDataSourceModule(),
                binder -> {
                    binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                    jsonCodecBinder(binder).bindJsonCodec(new TypeLiteral<Set<String>>() {});
                    jsonCodecBinder(binder).bindMapJsonCodec(String.class, String.class);
                    jsonCodecBinder(binder).bindListJsonCodec(QueryInputMetadata.class);
                    jsonCodecBinder(binder).bindJsonCodec(QueryOutputMetadata.class);
                    jsonCodecBinder(binder).bindListJsonCodec(TrinoWarning.class);
                    binder.bind(QueryDao.class).toProvider(QueryDaoProvider.class).in(Scopes.SINGLETON);
                    binder.bind(MysqlEventListener.class).in(Scopes.SINGLETON);
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(MysqlEventListener.class);
    }

    private static class MysqlDataSourceModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            configBinder(binder).bindConfig(MysqlEventListenerConfig.class);
        }

        @Singleton
        @Provides
        public ConnectionFactory createConnectionFactory(MysqlEventListenerConfig config)
        {
            return () -> new Driver().connect(config.getUrl(), new Properties());
        }

        @Singleton
        @Provides
        public static Jdbi createJdbi(ConnectionFactory connectionFactory, OpenTelemetry openTelemetry)
        {
            return Jdbi.create(connectionFactory)
                    .installPlugin(new SqlObjectPlugin())
                    .installPlugin(new JdbiOpenTelemetryPlugin(openTelemetry));
        }
    }

    private static class QueryDaoProvider
            implements Provider<QueryDao>
    {
        private final QueryDao dao;

        @Inject
        public QueryDaoProvider(Jdbi jdbi)
        {
            this.dao = jdbi
                    .installPlugin(new SqlObjectPlugin())
                    .onDemand(QueryDao.class);
        }

        @Override
        public QueryDao get()
        {
            return dao;
        }
    }
}
