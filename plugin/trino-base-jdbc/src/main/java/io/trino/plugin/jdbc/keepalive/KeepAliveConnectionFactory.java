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
package io.trino.plugin.jdbc.keepalive;

import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.base.inject.Decorator;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForwardingConnectionFactory;
import io.trino.plugin.jdbc.QueryConfig;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.plugin.jdbc.ReusableConnectionFactory.FactoryDecorator.REUSABLE_PRIORITY;
import static java.util.Objects.requireNonNull;

public class KeepAliveConnectionFactory
        extends ForwardingConnectionFactory
{
    private final ConnectionFactory delegate;
    private final Duration interval;
    private final ScheduledExecutorService executorService;

    public KeepAliveConnectionFactory(ConnectionFactory delegate, Duration interval, ScheduledExecutorService executorService)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.interval = requireNonNull(interval, "interval is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
    }

    @Override
    protected ConnectionFactory delegate()
    {
        return delegate;
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        return new KeepAliveConnection(super.openConnection(session), executorService, interval);
    }

    public static class FactoryDecorator
            implements Decorator<ConnectionFactory>
    {
        public static final int KEEP_ALIVE_PRIORITY = REUSABLE_PRIORITY + 1;
        private final Duration interval;

        @Inject
        public FactoryDecorator(QueryConfig config)
        {
            this.interval = config.getKeepAliveInterval();
        }

        @Override
        public int priority()
        {
            return KEEP_ALIVE_PRIORITY;
        }

        @Override
        public ConnectionFactory apply(ConnectionFactory delegate)
        {
            return new KeepAliveConnectionFactory(delegate, interval, Executors.newScheduledThreadPool(4, threadsNamed("jdbc-keep-alive-%d")));
        }
    }
}
