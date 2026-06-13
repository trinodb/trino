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
package io.trino.plugin.redshift;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRedshiftUnloadSplitSource
{
    @Test
    public void testCloseCancelsActiveUnloadStatement()
            throws Exception
    {
        BlockingPreparedStatement unloadStatement = new BlockingPreparedStatement();
        Connection connection = newProxy(Connection.class, (_, method, _) -> switch (method.getName()) {
            case "close", "setReadOnly" -> null;
            case "isClosed" -> false;
            case "toString" -> "connection";
            default -> defaultValue(method.getReturnType());
        });
        JdbcClient jdbcClient = newProxy(JdbcClient.class, (_, method, _) -> {
            if (method.getName().equals("getConnection")) {
                return connection;
            }
            return defaultValue(method.getReturnType());
        });
        QueryBuilder queryBuilder = newProxy(QueryBuilder.class, (_, method, _) -> {
            if (method.getName().equals("prepareStatement")) {
                return unloadStatement.statement();
            }
            return defaultValue(method.getReturnType());
        });
        TrinoFileSystem fileSystem = newProxy(TrinoFileSystem.class, (_, method, _) -> defaultValue(method.getReturnType()));

        try (ExecutorService executor = newSingleThreadExecutor(daemonThreadsNamed("TestRedshiftUnloadSplitSource-%s"))) {
            RedshiftUnloadSplitSource splitSource = new TestingRedshiftUnloadSplitSource(executor, jdbcClient, queryBuilder, fileSystem);
            try {
                assertThat(unloadStatement.awaitExecuteStarted()).isTrue();

                splitSource.close();

                assertThat(unloadStatement.isCancelCalled()).isTrue();
            }
            finally {
                unloadStatement.release();
                splitSource.close();
            }
        }
    }

    private static class TestingRedshiftUnloadSplitSource
            extends RedshiftUnloadSplitSource
    {
        TestingRedshiftUnloadSplitSource(ExecutorService executor, JdbcClient jdbcClient, QueryBuilder queryBuilder, TrinoFileSystem fileSystem)
        {
            super(
                    executor,
                    SESSION,
                    jdbcClient,
                    new JdbcTableHandle(
                            new SchemaTableName("schema", "table"),
                            new RemoteTableName(Optional.empty(), Optional.of("schema"), "table"),
                            Optional.empty()),
                    List.of(),
                    queryBuilder,
                    RemoteQueryModifier.NONE,
                    "s3://bucket",
                    Optional.empty(),
                    fileSystem);
        }

        @Override
        String buildRedshiftSelectSql(ConnectorSession session, Connection connection, JdbcTableHandle table, List<JdbcColumnHandle> columns)
        {
            return "SELECT 1";
        }
    }

    private static class BlockingPreparedStatement
    {
        private final CountDownLatch executeStarted = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicBoolean cancelCalled = new AtomicBoolean();
        private final PreparedStatement statement = newProxy(PreparedStatement.class, this::invoke);

        PreparedStatement statement()
        {
            return statement;
        }

        boolean awaitExecuteStarted()
                throws InterruptedException
        {
            return executeStarted.await(10, SECONDS);
        }

        boolean isCancelCalled()
        {
            return cancelCalled.get();
        }

        void release()
        {
            release.countDown();
        }

        private Object invoke(Object proxy, Method method, Object[] args)
                throws SQLException
        {
            if (method.getName().equals("execute") && method.getParameterCount() == 0) {
                executeStarted.countDown();
                try {
                    if (!release.await(10, SECONDS)) {
                        throw new SQLException("Timed out waiting for test to release statement");
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted while waiting for test to release statement", e);
                }
                return false;
            }
            if (method.getName().equals("cancel") && method.getParameterCount() == 0) {
                cancelCalled.set(true);
                release.countDown();
                return null;
            }
            if (method.getName().equals("close") && method.getParameterCount() == 0) {
                release.countDown();
                return null;
            }
            if (method.getName().equals("toString") && method.getParameterCount() == 0) {
                return "blocking statement";
            }
            return defaultValue(method.getReturnType());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T newProxy(Class<T> type, InvocationHandler handler)
    {
        return (T) Proxy.newProxyInstance(
                TestRedshiftUnloadSplitSource.class.getClassLoader(),
                new Class<?>[] {type},
                handler);
    }

    private static Object defaultValue(Class<?> type)
    {
        if (!type.isPrimitive() || type == void.class) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0.0f;
        }
        if (type == double.class) {
            return 0.0;
        }
        if (type == char.class) {
            return '\0';
        }
        throw new IllegalArgumentException("Unsupported primitive type: " + type);
    }
}
