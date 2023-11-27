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
package io.trino.plugin.jdbc.jmx;

import com.google.inject.Inject;
import io.trino.plugin.base.inject.Decorator;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForwardingConnectionFactory;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

public class StatisticsAwareConnectionFactory
        extends ForwardingConnectionFactory
{
    private final ConnectionFactory delegate;
    private final ConnectionFactoryStats stats;

    public StatisticsAwareConnectionFactory(ConnectionFactory delegate, ConnectionFactoryStats stats)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.stats = requireNonNull(stats, "stats is null");
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
        return stats.getOpenConnection().wrap(() -> delegate.openConnection(session));
    }

    @Override
    public void close()
            throws SQLException
    {
        stats.getCloseConnection().wrap(delegate::close);
    }

    public static class FactoryDecorator
            implements Decorator<ConnectionFactory>
    {
        public static final int STATISTICS_PRIORITY = 1;
        private final ConnectionFactoryStats stats;

        @Inject
        public FactoryDecorator(ConnectionFactoryStats stats)
        {
            this.stats = requireNonNull(stats, "stats is null");
        }

        @Override
        public int priority()
        {
            // Lowest priority wraps around the raw instance
            return STATISTICS_PRIORITY;
        }

        @Override
        public ConnectionFactory apply(ConnectionFactory delegate)
        {
            return new StatisticsAwareConnectionFactory(delegate, stats);
        }
    }
}
