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

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.trino.plugin.base.inject.Decorator;
import io.trino.spi.connector.ConnectorSession;
import jakarta.annotation.Nullable;

import java.sql.Connection;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.jdbc.jmx.StatisticsAwareConnectionFactory.FactoryDecorator.STATISTICS_PRIORITY;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class LazyConnectionFactory
        extends ForwardingConnectionFactory
{
    private final ConnectionFactory delegate;

    @Inject
    public LazyConnectionFactory(ConnectionFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
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
        return new LazyConnection(() -> delegate.openConnection(session));
    }

    private static final class LazyConnection
            extends ForwardingConnection
    {
        private final SqlSupplier<Connection> connectionSupplier;
        @Nullable
        @GuardedBy("this")
        private Connection connection;
        @GuardedBy("this")
        private boolean closed;

        public LazyConnection(SqlSupplier<Connection> connectionSupplier)
        {
            this.connectionSupplier = requireNonNull(connectionSupplier, "connectionSupplier is null");
        }

        @Override
        protected synchronized Connection delegate()
                throws SQLException
        {
            checkState(!closed, "Connection is already closed");
            if (connection == null) {
                connection = requireNonNull(connectionSupplier.get(), "connectionSupplier.get() is null");
            }
            return connection;
        }

        @Override
        public synchronized void close()
                throws SQLException
        {
            closed = true;
            if (connection != null) {
                connection.close();
            }
        }
    }

    @FunctionalInterface
    private interface SqlSupplier<T>
    {
        T get()
                throws SQLException;
    }

    public static class FactoryDecorator
            implements Decorator<ConnectionFactory>
    {
        public static final int LAZY_CONNECTION_PRIORITY = STATISTICS_PRIORITY + 1;

        @Override
        public int priority()
        {
            return LAZY_CONNECTION_PRIORITY;
        }

        @Override
        public ConnectionFactory apply(ConnectionFactory delegate)
        {
            return new LazyConnectionFactory(delegate);
        }
    }
}
