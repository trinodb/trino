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
package io.trino.plugin.ydb;

import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_OPERATION_NUMBER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestYdbMergeSink
{
    @Test
    public void testAllOperationsShareTransactionAndKeyOrder()
    {
        RecordingJdbc jdbc = new RecordingJdbc(0);
        YdbMergeSink sink = sink(jdbc);
        sink.storeMergedRows(page());
        assertThat(jdbc.commits).isZero();
        assertThat(jdbc.rollbacks).isZero();
        assertThat(jdbc.executions).hasSize(3);
        assertThat(jdbc.executions.get(0).parameters()).containsExactly(90L, 10L, 7L);
        assertThat(jdbc.executions.get(1).sql()).contains("`id` IS NOT DISTINCT FROM ? AND `tenant` IS NOT DISTINCT FROM ?");
        assertThat(jdbc.executions.get(1).parameters()).containsExactly(91L, 7L, null);
        assertThat(jdbc.executions.get(2).parameters()).containsExactly(8L, 11L);
        assertThat(sink.finish().join()).hasSize(1);
        assertThat(jdbc.commits).isEqualTo(1);
        assertThat(jdbc.closed).isTrue();
        assertThat(jdbc.closedStatements).isEqualTo(3);
        sink.abort();
        assertThat(jdbc.rollbacks).isZero();
    }

    @Test
    public void testFailureRollsBackAllOperationKinds()
    {
        RecordingJdbc jdbc = new RecordingJdbc(2);
        YdbMergeSink sink = sink(jdbc);
        assertThatThrownBy(() -> sink.storeMergedRows(page())).hasCause(jdbc.failure);
        assertThat(jdbc.executions).hasSize(2);
        assertThat(jdbc.commits).isZero();
        assertThat(jdbc.rollbacks).isEqualTo(1);
        assertThat(jdbc.closed).isTrue();
        assertThat(jdbc.closedStatements).isEqualTo(3);
        sink.abort();
        assertThat(jdbc.rollbacks).isEqualTo(1);
    }

    @Test
    public void testAbortRollsBackWithoutCommit()
    {
        RecordingJdbc jdbc = new RecordingJdbc(0);
        YdbMergeSink sink = sink(jdbc);
        sink.storeMergedRows(page());
        sink.abort();
        assertThat(jdbc.commits).isZero();
        assertThat(jdbc.rollbacks).isEqualTo(1);
        assertThat(jdbc.closed).isTrue();
        assertThat(jdbc.closedStatements).isEqualTo(3);
    }

    private static YdbMergeSink sink(RecordingJdbc jdbc)
    {
        YdbClient client = new YdbClient(
                new BaseJdbcConfig(),
                _ -> jdbc.connection(),
                new YdbQueryBuilder(RemoteQueryModifier.NONE),
                new DefaultIdentifierMapping(),
                RemoteQueryModifier.NONE)
        {
            @Override
            public Connection getConnection(ConnectorSession session, JdbcOutputTableHandle handle)
            {
                return jdbc.connection();
            }
        };
        List<JdbcColumnHandle> columns = List.of("payload", "tenant", "id").stream()
                .map(name -> new JdbcColumnHandle(name, YdbTypeUtils.toTypeHandle(BIGINT).orElseThrow(), BIGINT))
                .toList();
        RemoteTableName remote = new RemoteTableName(Optional.empty(), Optional.empty(), "test");
        JdbcOutputTableHandle output = new JdbcOutputTableHandle(
                remote,
                List.of("payload", "tenant", "id"),
                List.of(BIGINT, BIGINT, BIGINT),
                Optional.of(columns.stream().map(JdbcColumnHandle::getJdbcTypeHandle).toList()),
                Optional.empty(),
                Optional.empty());
        JdbcMergeTableHandle handle = new JdbcMergeTableHandle(
                new JdbcTableHandle(new SchemaTableName("default", "test"), remote, Optional.empty()),
                output,
                Map.of(),
                Optional.empty(),
                List.of(columns.get(2), columns.get(1)),
                columns,
                Map.of(0, List.of(columns.getFirst())));
        return new YdbMergeSink(SESSION, handle, client, () -> 123, RemoteQueryModifier.NONE);
    }

    private static Page page()
    {
        return new Page(
                numbers(BIGINT, 90L, 91L, 92L),
                numbers(BIGINT, 10L, null, 11L),
                numbers(BIGINT, 7L, 7L, 8L),
                numbers(TINYINT, (long) INSERT_OPERATION_NUMBER, (long) UPDATE_OPERATION_NUMBER, (long) DELETE_OPERATION_NUMBER),
                numbers(INTEGER, 0L, 0L, 0L),
                RowBlock.fromFieldBlocks(3, new Block[] {numbers(BIGINT, 7L, 7L, 8L), numbers(BIGINT, 10L, null, 11L)}));
    }

    private static Block numbers(Type type, Long... values)
    {
        var builder = type.createBlockBuilder(null, values.length);
        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, value);
            }
        }
        return builder.build();
    }

    private record Execution(String sql, List<Object> parameters) {}

    private static final class RecordingJdbc
    {
        private final int failOnExecution;
        private final SQLException failure = new SQLException("Injected statement failure");
        private final List<Execution> executions = new ArrayList<>();
        private int commits;
        private int rollbacks;
        private int closedStatements;
        private boolean closed;

        private RecordingJdbc(int failOnExecution)
        {
            this.failOnExecution = failOnExecution;
        }

        private Connection connection()
        {
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                    (_, method, args) -> switch (method.getName()) {
                        case "setAutoCommit" -> {
                            assertThat(args[0]).isEqualTo(false);
                            yield null;
                        }
                        case "prepareStatement" -> statement((String) args[0]);
                        case "commit" -> {
                            commits++;
                            yield null;
                        }
                        case "rollback" -> {
                            rollbacks++;
                            yield null;
                        }
                        case "close" -> {
                            closed = true;
                            yield null;
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

        private PreparedStatement statement(String sql)
        {
            Map<Integer, Object> parameters = new TreeMap<>();
            return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[] {PreparedStatement.class},
                    (_, method, args) -> switch (method.getName()) {
                        case "setLong", "setObject" -> {
                            parameters.put((int) args[0], args[1]);
                            yield null;
                        }
                        case "setNull" -> {
                            parameters.put((int) args[0], null);
                            yield null;
                        }
                        case "executeUpdate" -> {
                            executions.add(new Execution(sql, new ArrayList<>(parameters.values())));
                            if (executions.size() == failOnExecution) {
                                throw failure;
                            }
                            yield 1;
                        }
                        case "close" -> {
                            closedStatements++;
                            yield null;
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }
    }
}
