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

import io.trino.spi.connector.ConnectorSession;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class LazyConnectionFactory
        implements ConnectionFactory
{
    private final ConnectionFactory delegate;

    @Inject
    public LazyConnectionFactory(@ForLazyConnectionFactory ConnectionFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        return new LazyConnection(() -> delegate.openConnection(session));
    }

    @Override
    public void close()
            throws SQLException
    {
        delegate.close();
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
}
