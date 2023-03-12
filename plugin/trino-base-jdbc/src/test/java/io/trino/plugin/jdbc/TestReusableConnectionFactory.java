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
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.time.Duration;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Thread.sleep;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestReusableConnectionFactory
{
    private static final ConnectorSession ALICE = TestingConnectorSession.builder()
            .setIdentity(ConnectorIdentity.ofUser("alice"))
            .build();
    private static final ConnectorSession BOB = TestingConnectorSession.builder()
            .setIdentity(ConnectorIdentity.ofUser("bob"))
            .build();
    private static final Duration FOREVER = Duration.ofSeconds(5);
    private static final int HUGE_SIZE = 1000;

    @Test
    public void testConnectionIsReusedSingleUser()
            throws Exception
    {
        try (MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
                ReusableConnectionFactory connectionFactory = new ReusableConnectionFactory(mockConnectionFactory, FOREVER, HUGE_SIZE)) {
            connectionFactory.openConnection(ALICE).close();
            connectionFactory.openConnection(ALICE).close();
            connectionFactory.openConnection(ALICE).close();
            assertThat(mockConnectionFactory.openedConnections).isEqualTo(1);
        }
    }

    @Test
    public void testSingleUserCanCreateMultipleConnections()
            throws Exception
    {
        try (MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
                ReusableConnectionFactory connectionFactory = new ReusableConnectionFactory(mockConnectionFactory, FOREVER, HUGE_SIZE)) {
            Connection connection1 = connectionFactory.openConnection(ALICE);
            assertThat(mockConnectionFactory.openedConnections).isEqualTo(1);

            Connection connection2 = connectionFactory.openConnection(ALICE);
            assertThat(mockConnectionFactory.openedConnections).isEqualTo(2);

            connection1.close();
            assertThat(mockConnectionFactory.closedConnections).isEqualTo(0);

            connection2.close();
            assertThat(mockConnectionFactory.closedConnections).isEqualTo(1);
        }
    }

    @Test
    public void testConnectionIsReusedTwoUsers()
            throws Exception
    {
        try (MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
                ReusableConnectionFactory connectionFactory = new ReusableConnectionFactory(mockConnectionFactory, FOREVER, HUGE_SIZE)) {
            connectionFactory.openConnection(ALICE).close();
            connectionFactory.openConnection(BOB).close();
            connectionFactory.openConnection(BOB).close();
            connectionFactory.openConnection(ALICE).close();
            assertThat(mockConnectionFactory.openedConnections).isEqualTo(2);
        }
    }

    @Test
    public void testConnectionIsNotShared()
            throws Exception
    {
        try (MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
                ReusableConnectionFactory connectionFactory = new ReusableConnectionFactory(mockConnectionFactory, FOREVER, HUGE_SIZE);
                Connection ignored = connectionFactory.openConnection(ALICE)) {
            assertThat(mockConnectionFactory.openedConnections).isEqualTo(1);
            connectionFactory.openConnection(ALICE).close();
            assertThat(mockConnectionFactory.openedConnections).isEqualTo(2);
        }
    }

    @Test
    public void testConnectionIsEvicted()
            throws Exception
    {
        try (MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
                ReusableConnectionFactory connectionFactory = new ReusableConnectionFactory(mockConnectionFactory, Duration.ofMillis(50), HUGE_SIZE)) {
            connectionFactory.openConnection(ALICE).close();
            sleep(100);
            connectionFactory.openConnection(ALICE).close();
            assertThat(mockConnectionFactory.openedConnections).isEqualTo(2);
        }
    }

    @Test
    public void testNoNewConnectionIsReusedWhenCacheIsFull()
            throws Exception
    {
        try (MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
                ReusableConnectionFactory connectionFactory = new ReusableConnectionFactory(mockConnectionFactory, FOREVER, 1)) {
            connectionFactory.openConnection(ALICE).close();
            connectionFactory.openConnection(BOB).close();
            connectionFactory.openConnection(ALICE).close();
            connectionFactory.openConnection(BOB).close();
            connectionFactory.openConnection(ALICE).close();
            assertThat(mockConnectionFactory.openedConnections).isEqualTo(5);
        }
    }

    @Test
    public void testConnectionIsNotReusedWhenSetToReadOnly()
            throws Exception
    {
        try (MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
                ReusableConnectionFactory connectionFactory = new ReusableConnectionFactory(mockConnectionFactory, FOREVER, HUGE_SIZE)) {
            Connection connection = connectionFactory.openConnection(ALICE);
            connection.setReadOnly(true);
            connection.close();
            connectionFactory.openConnection(ALICE).close();
            assertThat(mockConnectionFactory.openedConnections).isEqualTo(2);
        }
    }

    @Test
    public void testConnectionIsNotReusedWhenDelegateIsClosed()
            throws Exception
    {
        MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
        ReusableConnectionFactory connectionFactory = new ReusableConnectionFactory(mockConnectionFactory, FOREVER, HUGE_SIZE);
        Connection connection = connectionFactory.openConnection(ALICE);

        Connection delegate = ((ReusableConnectionFactory.CachedConnection) connection).delegate();
        delegate.close();
        connection.close();

        Connection secondConnection = connectionFactory.openConnection(ALICE);
        Connection secondDelegate = ((ReusableConnectionFactory.CachedConnection) secondConnection).delegate();

        assertThat(delegate).isNotEqualTo(secondDelegate);
        secondConnection.close();

        assertThat(mockConnectionFactory.openedConnections).isEqualTo(2);
        assertThat(mockConnectionFactory.closedConnections).isEqualTo(1);
    }

    @Test
    public void testConnectionIsNotReusedWhenAutoCommitDisabled()
            throws Exception
    {
        try (MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
                ReusableConnectionFactory connectionFactory = new ReusableConnectionFactory(mockConnectionFactory, FOREVER, HUGE_SIZE)) {
            Connection connection = connectionFactory.openConnection(ALICE);
            connection.setAutoCommit(false);
            connection.close();
            connectionFactory.openConnection(ALICE).close();
            assertThat(mockConnectionFactory.openedConnections).isEqualTo(2);
        }
    }

    private static final class MockConnectionFactory
            implements ConnectionFactory
    {
        private int openedConnections;
        private int closedConnections;

        @Override
        public Connection openConnection(ConnectorSession session)
        {
            openedConnections++;
            return new MockConnection(() -> closedConnections++);
        }

        @Override
        public void close()
        {
            assertThat(openedConnections).isEqualTo(closedConnections);
        }
    }

    private static final class MockConnection
            extends ForwardingConnection
    {
        private final Runnable onClose;
        private volatile boolean closed;

        MockConnection(Runnable onClose)
        {
            this.onClose = requireNonNull(onClose, "onClose is null");
        }

        @Override
        protected Connection delegate()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isClosed()
        {
            return closed;
        }

        @Override
        public void setAutoCommit(boolean autoCommit) {}

        @Override
        public void setReadOnly(boolean readOnly) {}

        @Override
        public void close()
        {
            checkState(!closed, "Connection is already closed");
            closed = true;
            onClose.run();
        }
    }
}
