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

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.stream.Stream;

import static com.google.common.reflect.Reflection.newProxy;
import static io.prestosql.plugin.jdbc.TestRetryingConnectionFactory.MockConnectorFactory.Action.RETURN;
import static io.prestosql.plugin.jdbc.TestRetryingConnectionFactory.MockConnectorFactory.Action.THROW_NPE;
import static io.prestosql.plugin.jdbc.TestRetryingConnectionFactory.MockConnectorFactory.Action.THROW_PRESTO_EXCEPTION;
import static io.prestosql.plugin.jdbc.TestRetryingConnectionFactory.MockConnectorFactory.Action.THROW_SQL_EXCEPTION;
import static io.prestosql.plugin.jdbc.TestRetryingConnectionFactory.MockConnectorFactory.Action.THROW_SQL_RECOVERABLE_EXCEPTION;
import static io.prestosql.plugin.jdbc.TestRetryingConnectionFactory.MockConnectorFactory.Action.THROW_WRAPPED_SQL_RECOVERABLE_EXCEPTION;
import static io.prestosql.spi.block.TestingSession.SESSION;
import static io.prestosql.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestRetryingConnectionFactory
{
    private static final JdbcIdentity IDENTITY = JdbcIdentity.from(SESSION);

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectionFactory.class, RetryingConnectionFactory.class);
    }

    @Test
    public void testSimplyReturnConnection()
            throws Exception
    {
        MockConnectorFactory mock = new MockConnectorFactory(RETURN);
        ConnectionFactory factory = new RetryingConnectionFactory(mock);
        assertNotNull(factory.openConnection(IDENTITY));
        assertEquals(mock.getCallCount(), 1);
    }

    @Test
    public void testRetryAndStopOnPrestoException()
    {
        MockConnectorFactory mock = new MockConnectorFactory(THROW_SQL_RECOVERABLE_EXCEPTION, THROW_PRESTO_EXCEPTION);
        ConnectionFactory factory = new RetryingConnectionFactory(mock);
        assertThatThrownBy(() -> factory.openConnection(IDENTITY))
                .isInstanceOf(PrestoException.class)
                .hasMessage("Testing presto exception");
        assertEquals(mock.getCallCount(), 2);
    }

    @Test
    public void testRetryAndStopOnSqlException()
    {
        MockConnectorFactory mock = new MockConnectorFactory(THROW_SQL_RECOVERABLE_EXCEPTION, THROW_SQL_EXCEPTION);
        ConnectionFactory factory = new RetryingConnectionFactory(mock);
        assertThatThrownBy(() -> factory.openConnection(IDENTITY))
                .isInstanceOf(SQLException.class)
                .hasMessage("Testing sql exception");
        assertEquals(mock.getCallCount(), 2);
    }

    @Test
    public void testNullPointerException()
    {
        MockConnectorFactory mock = new MockConnectorFactory(THROW_NPE);
        ConnectionFactory factory = new RetryingConnectionFactory(mock);
        assertThatThrownBy(() -> factory.openConnection(IDENTITY))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Testing NPE");
        assertEquals(mock.getCallCount(), 1);
    }

    @Test
    public void testRetryAndReturn()
            throws Exception
    {
        MockConnectorFactory mock = new MockConnectorFactory(THROW_SQL_RECOVERABLE_EXCEPTION, RETURN);
        ConnectionFactory factory = new RetryingConnectionFactory(mock);
        assertNotNull(factory.openConnection(IDENTITY));
        assertEquals(mock.getCallCount(), 2);
    }

    @Test
    public void testRetryOnWrappedAndReturn()
            throws Exception
    {
        MockConnectorFactory mock = new MockConnectorFactory(THROW_WRAPPED_SQL_RECOVERABLE_EXCEPTION, RETURN);
        ConnectionFactory factory = new RetryingConnectionFactory(mock);
        assertNotNull(factory.openConnection(IDENTITY));
        assertEquals(mock.getCallCount(), 2);
    }

    public static class MockConnectorFactory
            implements ConnectionFactory
    {
        private final Deque<Action> actions = new ArrayDeque<>();
        private int callCount;

        public MockConnectorFactory(Action... actions)
        {
            Stream.of(actions)
                    .forEach(this.actions::push);
        }

        public int getCallCount()
        {
            return callCount;
        }

        @Override
        public Connection openConnection(JdbcIdentity identity)
                throws SQLException
        {
            callCount++;
            Action action = requireNonNull(actions.pollLast(), "actions.pollFirst() is null");
            switch (action) {
                case RETURN:
                    return newProxy(Connection.class, (proxy, method, args) -> null);
                case THROW_NPE:
                    throw new NullPointerException("Testing NPE");
                case THROW_PRESTO_EXCEPTION:
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Testing presto exception");
                case THROW_SQL_EXCEPTION:
                    throw new SQLException("Testing sql exception");
                case THROW_SQL_RECOVERABLE_EXCEPTION:
                    throw new SQLRecoverableException("Testing sql recoverable exception");
                case THROW_WRAPPED_SQL_RECOVERABLE_EXCEPTION:
                    throw new RuntimeException(new SQLRecoverableException("Testing sql recoverable exception"));
            }
            throw new IllegalStateException("Unsupported action:" + action);
        }

        public enum Action
        {
            THROW_PRESTO_EXCEPTION,
            THROW_SQL_EXCEPTION,
            THROW_SQL_RECOVERABLE_EXCEPTION,
            THROW_WRAPPED_SQL_RECOVERABLE_EXCEPTION,
            THROW_NPE,
            RETURN,
        }
    }
}
