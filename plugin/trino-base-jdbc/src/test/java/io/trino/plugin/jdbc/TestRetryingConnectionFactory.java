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

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.stream.Stream;

import static com.google.common.reflect.Reflection.newProxy;
import static io.trino.plugin.jdbc.TestRetryingConnectionFactory.MockConnectorFactory.Action.RETURN;
import static io.trino.plugin.jdbc.TestRetryingConnectionFactory.MockConnectorFactory.Action.THROW_NPE;
import static io.trino.plugin.jdbc.TestRetryingConnectionFactory.MockConnectorFactory.Action.THROW_SQL_EXCEPTION;
import static io.trino.plugin.jdbc.TestRetryingConnectionFactory.MockConnectorFactory.Action.THROW_SQL_RECOVERABLE_EXCEPTION;
import static io.trino.plugin.jdbc.TestRetryingConnectionFactory.MockConnectorFactory.Action.THROW_TRINO_EXCEPTION;
import static io.trino.plugin.jdbc.TestRetryingConnectionFactory.MockConnectorFactory.Action.THROW_WRAPPED_SQL_RECOVERABLE_EXCEPTION;
import static io.trino.spi.block.TestingSession.SESSION;
import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRetryingConnectionFactory
{
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
        assertThat(factory.openConnection(SESSION)).isNotNull();
        assertThat(mock.getCallCount()).isEqualTo(1);
    }

    @Test
    public void testRetryAndStopOnTrinoException()
    {
        MockConnectorFactory mock = new MockConnectorFactory(THROW_SQL_RECOVERABLE_EXCEPTION, THROW_TRINO_EXCEPTION);
        ConnectionFactory factory = new RetryingConnectionFactory(mock);
        assertThatThrownBy(() -> factory.openConnection(SESSION))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Testing Trino exception");
        assertThat(mock.getCallCount()).isEqualTo(2);
    }

    @Test
    public void testRetryAndStopOnSqlException()
    {
        MockConnectorFactory mock = new MockConnectorFactory(THROW_SQL_RECOVERABLE_EXCEPTION, THROW_SQL_EXCEPTION);
        ConnectionFactory factory = new RetryingConnectionFactory(mock);
        assertThatThrownBy(() -> factory.openConnection(SESSION))
                .isInstanceOf(SQLException.class)
                .hasMessage("Testing sql exception");
        assertThat(mock.getCallCount()).isEqualTo(2);
    }

    @Test
    public void testNullPointerException()
    {
        MockConnectorFactory mock = new MockConnectorFactory(THROW_NPE);
        ConnectionFactory factory = new RetryingConnectionFactory(mock);
        assertThatThrownBy(() -> factory.openConnection(SESSION))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Testing NPE");
        assertThat(mock.getCallCount()).isEqualTo(1);
    }

    @Test
    public void testRetryAndReturn()
            throws Exception
    {
        MockConnectorFactory mock = new MockConnectorFactory(THROW_SQL_RECOVERABLE_EXCEPTION, RETURN);
        ConnectionFactory factory = new RetryingConnectionFactory(mock);
        assertThat(factory.openConnection(SESSION)).isNotNull();
        assertThat(mock.getCallCount()).isEqualTo(2);
    }

    @Test
    public void testRetryOnWrappedAndReturn()
            throws Exception
    {
        MockConnectorFactory mock = new MockConnectorFactory(THROW_WRAPPED_SQL_RECOVERABLE_EXCEPTION, RETURN);
        ConnectionFactory factory = new RetryingConnectionFactory(mock);
        assertThat(factory.openConnection(SESSION)).isNotNull();
        assertThat(mock.getCallCount()).isEqualTo(2);
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
        public Connection openConnection(ConnectorSession session)
                throws SQLException
        {
            callCount++;
            Action action = requireNonNull(actions.pollLast(), "actions.pollFirst() is null");
            switch (action) {
                case RETURN:
                    return newProxy(Connection.class, (proxy, method, args) -> null);
                case THROW_NPE:
                    throw new NullPointerException("Testing NPE");
                case THROW_TRINO_EXCEPTION:
                    throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "Testing Trino exception");
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
            THROW_TRINO_EXCEPTION,
            THROW_SQL_EXCEPTION,
            THROW_SQL_RECOVERABLE_EXCEPTION,
            THROW_WRAPPED_SQL_RECOVERABLE_EXCEPTION,
            THROW_NPE,
            RETURN,
        }
    }
}
