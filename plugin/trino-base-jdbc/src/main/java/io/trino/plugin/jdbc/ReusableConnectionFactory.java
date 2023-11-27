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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.plugin.base.inject.Decorator;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.cache.RemovalCause.EXPLICIT;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.RetryingConnectionFactory.FactoryDecorator.RETRYING_PRIORITY;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class ReusableConnectionFactory
        extends ForwardingConnectionFactory
{
    @GuardedBy("this")
    private final Cache<String, Connection> connections;
    private ConnectionFactory delegate;

    public ReusableConnectionFactory(ConnectionFactory delegate, DelegatingListener listener)
    {
        this(delegate, Duration.ofSeconds(2), 10);
        listener.setFactory(this);
    }

    ReusableConnectionFactory(ConnectionFactory delegate, Duration duration, long maximumSize)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.connections = createConnectionsCache(duration, maximumSize);
    }

    @Override
    protected ConnectionFactory delegate()
    {
        return delegate;
    }

    // CacheBuilder.build(CacheLoader) is forbidden, because it does not support eviction for ongoing loads.
    // In this class, loading is not used and cache is used more as a map. So this is safe.
    @SuppressModernizer
    private static Cache<String, Connection> createConnectionsCache(Duration duration, long maximumSize)
    {
        requireNonNull(duration, "duration is null");
        return CacheBuilder.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterWrite(duration)
                .removalListener(ReusableConnectionFactory::onRemoval)
                .build();
    }

    private static void onRemoval(RemovalNotification<String, Connection> notification)
    {
        if (notification.getCause() == EXPLICIT) {
            // connection was taken from the cache
            return;
        }
        try {
            requireNonNull(notification.getValue(), "notification.getValue() is null");
            notification.getValue().close();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        String queryId = session.getQueryId();
        Connection connection = getConnection(session, queryId);
        return new CachedConnection(queryId, connection);
    }

    private Connection getConnection(ConnectorSession session, String queryId)
            throws SQLException
    {
        Connection connection = connections.asMap().remove(queryId);
        if (connection != null) {
            return connection;
        }
        return super.openConnection(session);
    }

    public void cleanupQuery(ConnectorSession session)
    {
        Connection connection = connections.asMap().remove(session.getQueryId());
        if (connection != null) {
            try {
                connection.close();
            }
            catch (SQLException e) {
                throw new TrinoException(JDBC_ERROR, e);
            }
        }
    }

    @Override
    public void close()
            throws SQLException
    {
        for (Connection connection : connections.asMap().values()) {
            connection.close();
        }
        connections.invalidateAll();
        super.close();
    }

    public static class DelegatingListener
            implements JdbcQueryEventListener
    {
        private ReusableConnectionFactory factory;

        public void setFactory(ReusableConnectionFactory factory)
        {
            this.factory = requireNonNull(factory, "factory is null");
        }

        @Override
        public void beginQuery(ConnectorSession session)
        {
            // noop
        }

        @Override
        public void cleanupQuery(ConnectorSession session)
        {
            checkState(factory != null, "factory was null");
            factory.cleanupQuery(session);
        }
    }

    final class CachedConnection
            extends ForwardingConnection
    {
        private final String queryId;
        private final Connection delegate;
        private volatile boolean closed;
        private volatile boolean dirty;

        private CachedConnection(String queryId, Connection delegate)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        protected Connection delegate()
        {
            checkState(!closed, "Connection is already closed");
            return delegate;
        }

        @Override
        public void setAutoCommit(boolean autoCommit)
                throws SQLException
        {
            dirty = true;
            super.setAutoCommit(autoCommit);
        }

        @Override
        public void setReadOnly(boolean readOnly)
                throws SQLException
        {
            dirty = true;
            super.setReadOnly(readOnly);
        }

        @Override
        public void close()
                throws SQLException
        {
            if (closed) {
                return;
            }
            closed = true;
            if (dirty) {
                delegate.close();
            }
            else if (!delegate.isClosed()) {
                connections.put(queryId, delegate);
            }
        }
    }

    public static class FactoryDecorator
            implements Decorator<ConnectionFactory>
    {
        public static final int REUSABLE_PRIORITY = RETRYING_PRIORITY + 1;
        private final DelegatingListener listener;

        public FactoryDecorator(DelegatingListener listener)
        {
            this.listener = requireNonNull(listener, "listener is null");
        }

        @Override
        public int priority()
        {
            return REUSABLE_PRIORITY;
        }

        @Override
        public ConnectionFactory apply(ConnectionFactory delegate)
        {
            return new ReusableConnectionFactory(delegate, listener);
        }
    }
}
