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

import io.trino.Session;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.SystemSessionProperties.TASK_MAX_WRITER_COUNT;
import static java.util.Collections.synchronizedMap;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseJdbcConnectionCreationTest
        extends AbstractTestQueryFramework
{
    protected ConnectionCountingConnectionFactory connectionFactory;

    @BeforeAll
    public void verifySetup()
    {
        // Test expects connectionFactory to be provided with AbstractTestQueryFramework.createQueryRunner implementation
        requireNonNull(connectionFactory, "connectionFactory is null");
        connectionFactory.assertThatNoConnectionHasLeaked();
    }

    @AfterAll
    public void destroy()
            throws Exception
    {
        connectionFactory.close();
        connectionFactory = null;
    }

    protected void assertJdbcConnections(@Language("SQL") String query, int expectedJdbcConnectionsCount, Optional<String> errorMessage)
    {
        assertJdbcConnections(getSession(), query, expectedJdbcConnectionsCount, errorMessage);
    }

    protected void assertJdbcConnections(Session session, @Language("SQL") String query, int expectedJdbcConnectionsCount, Optional<String> errorMessage)
    {
        int before = connectionFactory.connectionCreationCount();
        if (errorMessage.isPresent()) {
            assertQueryFails(query, errorMessage.get());
        }
        else {
            // Disabling writers scaling to make expected number of opened connections constant
            Session querySession = Session.builder(session)
                    .setSystemProperty(TASK_MAX_WRITER_COUNT, "4")
                    .build();
            getQueryRunner().execute(querySession, query);
        }
        int after = connectionFactory.connectionCreationCount();
        try {
            assertThat(after - before)
                    .as("JDBC connections created for query: %s", query)
                    .isEqualTo(expectedJdbcConnectionsCount);
        }
        catch (AssertionError failure) {
            connectionFactory.addConnectionCreationStackTraces(failure, before, after);
            throw failure;
        }
        finally {
            connectionFactory.clearConnectionCreationStackTraces(after);
        }
        connectionFactory.assertThatNoConnectionHasLeaked();
    }

    protected static class ConnectionCountingConnectionFactory
            implements ConnectionFactory
    {
        // Map from connection to a fake exception (holds stacktrace) pointing to the place where the connection was created
        private final Map<Connection, Exception> connectionCreations = synchronizedMap(new IdentityHashMap<>());
        private final Map<Integer, Exception> connectionCreationStackTraces = new HashMap<>();
        private final ConnectionFactory delegate;
        private int createdConnections;

        public ConnectionCountingConnectionFactory(DriverConnectionFactory delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public Connection openConnection(ConnectorSession session)
                throws SQLException
        {
            Exception connectionCreation = recordConnectionCreation();
            Connection connection = delegate.openConnection(session);
            Exception previous = connectionCreations.put(connection, connectionCreation);
            if (previous != null) {
                // connectionCreations do not support two connections at a time yet
                IllegalStateException exception = new IllegalStateException("Two connections are opened for same session");
                exception.addSuppressed(previous);
                throw exception;
            }
            return new ForwardingConnection()
            {
                private volatile boolean closed;

                @Override
                protected Connection delegate()
                {
                    return connection;
                }

                @Override
                public void close()
                        throws SQLException
                {
                    if (closed) {
                        return;
                    }
                    closed = true;
                    verify(connectionCreations.remove(connection) != null, "Connection was not created with ConnectionCountingConnectionFactory: %s", connection);
                    super.close();
                }
            };
        }

        private synchronized int connectionCreationCount()
        {
            return createdConnections;
        }

        private synchronized Exception recordConnectionCreation()
        {
            createdConnections++;
            Exception connectionCreation = new Exception("JDBC connection creation %s".formatted(createdConnections));
            connectionCreationStackTraces.put(createdConnections, connectionCreation);
            return connectionCreation;
        }

        private synchronized void addConnectionCreationStackTraces(AssertionError failure, int before, int after)
        {
            for (int connectionNumber = before + 1; connectionNumber <= after; connectionNumber++) {
                Exception connectionCreation = connectionCreationStackTraces.get(connectionNumber);
                if (connectionCreation != null) {
                    failure.addSuppressed(connectionCreation);
                }
            }
        }

        private synchronized void clearConnectionCreationStackTraces(int throughConnectionNumber)
        {
            connectionCreationStackTraces.keySet().removeIf(connectionNumber -> connectionNumber <= throughConnectionNumber);
        }

        private void assertThatNoConnectionHasLeaked()
        {
            if (!connectionCreations.isEmpty()) {
                AssertionError error = new AssertionError("%s connections leaked, see attached places".formatted(connectionCreations.size()));
                connectionCreations.values().forEach(error::addSuppressed);
                throw error;
            }
        }

        @Override
        public void close()
                throws SQLException
        {
            delegate.close();
        }
    }
}
