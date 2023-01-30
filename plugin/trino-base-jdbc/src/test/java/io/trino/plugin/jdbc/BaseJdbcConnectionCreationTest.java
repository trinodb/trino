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
import io.trino.testing.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Verify.verify;
import static java.util.Collections.synchronizedMap;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true) // this class is stateful, see fields
public abstract class BaseJdbcConnectionCreationTest
        extends AbstractTestQueryFramework
{
    protected ConnectionCountingConnectionFactory connectionFactory;

    @BeforeClass
    public void verifySetup()
    {
        // Test expects connectionFactory to be provided with AbstractTestQueryFramework.createQueryRunner implementation
        requireNonNull(connectionFactory, "connectionFactory is null");
        connectionFactory.assertThatNoConnectionHasLeaked();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws Exception
    {
        connectionFactory.close();
        connectionFactory = null;
    }

    protected void assertJdbcConnections(@Language("SQL") String query, int expectedJdbcConnectionsCount, Optional<String> errorMessage)
    {
        int before = connectionFactory.openConnections.get();
        if (errorMessage.isPresent()) {
            assertQueryFails(query, errorMessage.get());
        }
        else {
            getQueryRunner().execute(query);
        }
        int after = connectionFactory.openConnections.get();
        assertThat(after - before).isEqualTo(expectedJdbcConnectionsCount);
        connectionFactory.assertThatNoConnectionHasLeaked();
    }

    protected static class ConnectionCountingConnectionFactory
            implements ConnectionFactory
    {
        // Map from connection to a fake exception (holds stacktrace) pointing to the place where the connection was created
        private final Map<Connection, Exception> connectionCreations = synchronizedMap(new IdentityHashMap<>());
        private final AtomicInteger openConnections = new AtomicInteger();
        private final ConnectionFactory delegate;

        public ConnectionCountingConnectionFactory(DriverConnectionFactory delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public Connection openConnection(ConnectorSession session)
                throws SQLException
        {
            openConnections.incrementAndGet();
            Connection connection = delegate.openConnection(session);
            Exception previous = connectionCreations.put(connection, new Exception("STACKTRACE"));
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
                    verify(connectionCreations.remove(connection) != null, "Connection was not created with ConnectionCountingConnectionFactory: " + connection);
                    super.close();
                }
            };
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
