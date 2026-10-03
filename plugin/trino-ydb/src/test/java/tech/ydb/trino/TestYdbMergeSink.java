package tech.ydb.trino;

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

public class TestYdbMergeSink {
    @Test
    public void testOperationOrderCompositeNullKeysAndResourceClosure() {
        RecordingJdbc jdbc = new RecordingJdbc(0);
        YdbMergeSink sink = sink(jdbc, 1000);
        assertThat(jdbc.connections).isZero();
        sink.storeMergedRows(page());
        assertThat(jdbc.executions).hasSize(4);
        assertThat(jdbc.executions.get(0).parameters()).containsExactly(90L, 10L, 7L);
        assertThat(jdbc.executions.get(1).sql()).contains("`id` = ? AND `tenant` IS NULL");
        assertThat(jdbc.executions.get(1).parameters()).containsExactly(91L, 7L);
        assertThat(jdbc.executions.get(2).parameters()).containsExactly(92L, 11L, 8L);
        assertThat(jdbc.executions.get(3).parameters()).containsExactly(9L, 12L);
        assertThat(jdbc.commits).isEqualTo(1);
        assertThat(jdbc.rollbacks).isZero();
        assertThat(jdbc.connections).isEqualTo(1);
        assertThat(jdbc.closedConnections).isEqualTo(1);
        assertThat(jdbc.closedStatements).isEqualTo(3);
        assertThat(sink.finish().join()).hasSize(1);
        sink.abort();
        assertThat(jdbc.commits).isEqualTo(1);
        assertThat(jdbc.connections).isEqualTo(1);
    }

    @Test
    public void testFailureRollsBackCurrentBatchWithoutReplay() {
        RecordingJdbc jdbc = new RecordingJdbc(2);
        YdbMergeSink sink = sink(jdbc, 1000);
        assertThatThrownBy(() -> sink.storeMergedRows(page())).hasCause(jdbc.failure);
        assertThat(jdbc.executions).hasSize(2);
        assertThat(jdbc.commits).isZero();
        assertThat(jdbc.rollbacks).isEqualTo(1);
        assertThat(jdbc.connections).isEqualTo(1);
        assertThat(jdbc.closedConnections).isEqualTo(1);
        assertThat(jdbc.closedStatements).isEqualTo(2);
        sink.abort();
        assertThat(jdbc.rollbacks).isEqualTo(1);
    }

    @Test
    public void testBoundedNonTransactionalBatches() {
        RecordingJdbc jdbc = new RecordingJdbc(4);
        YdbMergeSink sink = sink(jdbc, 2);
        assertThatThrownBy(() -> sink.storeMergedRows(page())).hasCause(jdbc.failure);
        assertThat(jdbc.executions).hasSize(4);
        assertThat(jdbc.commits).isEqualTo(1);
        assertThat(jdbc.rollbacks).isEqualTo(1);
        assertThat(jdbc.connections).isEqualTo(2);
        assertThat(jdbc.closedConnections).isEqualTo(2);
        assertThat(jdbc.closedStatements).isEqualTo(4);
    }

    @Test
    public void testAbortBeforeInputHasNoConnectionToLeak() {
        RecordingJdbc jdbc = new RecordingJdbc(0);
        YdbMergeSink sink = sink(jdbc, 1000);
        sink.abort();
        assertThat(jdbc.connections).isZero();
        assertThatThrownBy(() -> sink.storeMergedRows(page())).isInstanceOf(IllegalStateException.class);
    }

    private static YdbMergeSink sink(RecordingJdbc jdbc, int batchSize) {
        YdbClient client = new YdbClient(new BaseJdbcConfig(), _ -> jdbc.connection(),
                new YdbQueryBuilder(RemoteQueryModifier.NONE), new DefaultIdentifierMapping(), RemoteQueryModifier.NONE);
        List<JdbcColumnHandle> columns = List.of("payload", "tenant", "id").stream()
                .map(name -> new JdbcColumnHandle(name, YdbTypeUtils.toTypeHandle(BIGINT).orElseThrow(), BIGINT))
                .toList();
        RemoteTableName remote = new RemoteTableName(Optional.empty(), Optional.empty(), "test");
        JdbcOutputTableHandle output = new JdbcOutputTableHandle(remote, List.of("payload", "tenant", "id"),
                List.of(BIGINT, BIGINT, BIGINT), Optional.of(columns.stream().map(JdbcColumnHandle::getJdbcTypeHandle).toList()),
                Optional.empty(), Optional.empty());
        JdbcMergeTableHandle handle = new JdbcMergeTableHandle(
                new JdbcTableHandle(new SchemaTableName("default", "test"), remote, Optional.empty()), output, Map.of(), Optional.empty(),
                List.of(columns.get(2), columns.get(1)), columns, Map.of(0, List.of(columns.getFirst())));
        return new YdbMergeSink(SESSION, handle, client, () -> 123, RemoteQueryModifier.NONE, batchSize);
    }

    private static Page page() {
        return new Page(
                numbers(BIGINT, 90L, 91L, 92L, 93L),
                numbers(BIGINT, 10L, null, 11L, 12L),
                numbers(BIGINT, 7L, 7L, 8L, 9L),
                numbers(TINYINT, (long) INSERT_OPERATION_NUMBER, (long) UPDATE_OPERATION_NUMBER,
                        (long) INSERT_OPERATION_NUMBER, (long) DELETE_OPERATION_NUMBER),
                numbers(INTEGER, 0L, 0L, 0L, 0L),
                RowBlock.fromFieldBlocks(4, new Block[]{numbers(BIGINT, 7L, 7L, 8L, 9L), numbers(BIGINT, 10L, null, 11L, 12L)}));
    }

    private static Block numbers(Type type, Long... values) {
        var builder = type.createBlockBuilder(null, values.length);
        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            } else {
                type.writeLong(builder, value);
            }
        }
        return builder.build();
    }

    private record Execution(String sql, List<Object> parameters) {}

    private static final class RecordingJdbc {
        private final int failOnExecution;
        private final SQLException failure = new SQLException("Injected statement failure");
        private final List<Execution> executions = new ArrayList<>();
        private int commits;
        private int rollbacks;
        private int connections;
        private int closedConnections;
        private int closedStatements;

        private RecordingJdbc(int failOnExecution) {
            this.failOnExecution = failOnExecution;
        }

        private Connection connection() {
            connections++;
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
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
                            closedConnections++;
                            yield null;
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

        private PreparedStatement statement(String sql) {
            Map<Integer, Object> parameters = new TreeMap<>();
            List<List<Object>> batch = new ArrayList<>();
            return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                    (_, method, args) -> switch (method.getName()) {
                        case "setLong", "setObject" -> {
                            parameters.put((int) args[0], args[1]);
                            yield null;
                        }
                        case "setNull" -> {
                            parameters.put((int) args[0], null);
                            yield null;
                        }
                        case "addBatch" -> {
                            batch.add(new ArrayList<>(parameters.values()));
                            yield null;
                        }
                        case "clearBatch" -> {
                            batch.clear();
                            yield null;
                        }
                        case "executeBatch" -> {
                            for (List<Object> row : batch) {
                                executions.add(new Execution(sql, row));
                                if (executions.size() == failOnExecution) {
                                    throw failure;
                                }
                            }
                            yield new int[batch.size()];
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
