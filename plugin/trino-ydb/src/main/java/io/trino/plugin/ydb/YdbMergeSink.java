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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.jdbc.BooleanWriteFunction;
import io.trino.plugin.jdbc.DoubleWriteFunction;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * Implements explicitly non-transactional MERGE with bounded, owned batch transactions.
 * No connection or statement is retained between storeMergedRows calls. This is also
 * important on Trino 483, where MergeWriterOperator.close does not invoke sink.abort.
 */
final class YdbMergeSink
        implements ConnectorMergeSink
{
    private final ConnectorSession session;
    private final JdbcMergeTableHandle handle;
    private final JdbcClient client;
    private final ConnectorPageSinkId pageSinkId;
    private final RemoteQueryModifier modifier;
    private final int batchSize;
    private final List<JdbcColumnHandle> columns;
    private final List<JdbcColumnHandle> primaryKeys;
    private final Map<Integer, List<Integer>> updateChannels;
    private boolean finished;

    YdbMergeSink(
            ConnectorSession session,
            JdbcMergeTableHandle handle,
            JdbcClient client,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier modifier,
            int batchSize)
    {
        this.session = session;
        this.handle = handle;
        this.client = client;
        this.pageSinkId = pageSinkId;
        this.modifier = modifier;
        this.batchSize = batchSize;
        columns = handle.getDataColumns();
        primaryKeys = handle.getPrimaryKeys();
        updateChannels = handle.getUpdateCaseColumns().entrySet().stream().collect(toMap(
                Map.Entry::getKey,
                entry -> IntStream.range(0, columns.size())
                        .filter(index -> entry.getValue().contains(columns.get(index)))
                        .boxed()
                        .toList()));
    }

    @Override
    public void storeMergedRows(Page page)
    {
        checkState(!finished, "MERGE sink is finished");
        checkArgument(page.getChannelCount() == columns.size() + 3, "Invalid MERGE page channel count");
        for (int offset = 0; offset < page.getPositionCount(); offset += batchSize) {
            writeBatch(page.getRegion(offset, Math.min(batchSize, page.getPositionCount() - offset)));
        }
    }

    private void writeBatch(Page page)
    {
        try (Connection connection = client.getConnection(session, handle.getOutputTableHandle())) {
            connection.setAutoCommit(false);
            Map<StatementKey, PreparedStatement> statements = new HashMap<>();
            try {
                List<WriteFunction> columnWriters = writers(connection, columns);
                List<WriteFunction> keyWriters = writers(connection, primaryKeys);
                List<Block> rowIdFields = RowBlock.getRowFieldsFromBlock(page.getBlock(columns.size() + 2));
                PreparedStatement pending = null;
                for (int position = 0; position < page.getPositionCount(); position++) {
                    int operation = TINYINT.getByte(page.getBlock(columns.size()), position);
                    int updateCase = operation == UPDATE_OPERATION_NUMBER
                            ? INTEGER.getInt(page.getBlock(columns.size() + 1), position) : -1;
                    ImmutableSet.Builder<Integer> nullKeys = ImmutableSet.builder();
                    if (operation != INSERT_OPERATION_NUMBER) {
                        for (int index = 0; index < primaryKeys.size(); index++) {
                            if (rowIdFields.get(index).isNull(position)) {
                                nullKeys.add(index);
                            }
                        }
                    }
                    StatementKey key = new StatementKey(operation, updateCase, nullKeys.build());
                    PreparedStatement statement = statements.get(key);
                    if (statement == null) {
                        statement = connection.prepareStatement(modifier.apply(session, sql(key, columnWriters, keyWriters)));
                        statements.put(key, statement);
                    }
                    if (pending != null && pending != statement) {
                        pending.executeBatch();
                        pending.clearBatch();
                    }
                    pending = statement;
                    int parameter = 1;
                    switch (operation) {
                        case INSERT_OPERATION_NUMBER -> {
                            for (int channel = 0; channel < columns.size(); channel++) {
                                bind(statement, parameter++, columns.get(channel).getColumnType(), columnWriters.get(channel), page.getBlock(channel), position);
                            }
                        }
                        case UPDATE_OPERATION_NUMBER -> {
                            for (int channel : updateChannels.get(updateCase)) {
                                bind(statement, parameter++, columns.get(channel).getColumnType(), columnWriters.get(channel), page.getBlock(channel), position);
                            }
                        }
                        case DELETE_OPERATION_NUMBER -> {}
                        default -> throw new IllegalArgumentException("Unknown MERGE operation: " + operation);
                    }
                    if (operation != INSERT_OPERATION_NUMBER) {
                        for (int index = 0; index < primaryKeys.size(); index++) {
                            if (!key.nullKeys().contains(index)) {
                                bind(statement, parameter++, primaryKeys.get(index).getColumnType(), keyWriters.get(index), rowIdFields.get(index), position);
                            }
                        }
                    }
                    statement.addBatch();
                }
                if (pending != null) {
                    pending.executeBatch();
                }
                connection.commit();
            }
            catch (SQLException | RuntimeException e) {
                try {
                    connection.rollback();
                }
                catch (SQLException rollbackFailure) {
                    e.addSuppressed(rollbackFailure);
                }
                closeAllSuppress(e, statements.values().toArray(PreparedStatement[]::new));
                throw e;
            }
            SQLException closeFailure = new SQLException("Failed to close YDB MERGE statements");
            closeAllSuppress(closeFailure, statements.values().toArray(PreparedStatement[]::new));
            if (closeFailure.getSuppressed().length > 0) {
                throw closeFailure;
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "YDB MERGE batch failed", e);
        }
    }

    private String sql(StatementKey key, List<WriteFunction> columnWriters, List<WriteFunction> keyWriters)
    {
        if (key.operation() == INSERT_OPERATION_NUMBER) {
            return client.buildInsertSql(handle.getOutputTableHandle(), columnWriters);
        }
        String table = client.quoted(handle.getOutputTableHandle().getRemoteTableName());
        String predicate = IntStream.range(0, primaryKeys.size())
                .mapToObj(index -> client.quoted(primaryKeys.get(index).getColumnName()) +
                        (key.nullKeys().contains(index) ? " IS NULL" : " = " + keyWriters.get(index).getBindExpression()))
                .collect(joining(" AND "));
        if (key.operation() == DELETE_OPERATION_NUMBER) {
            return "DELETE FROM " + table + " WHERE " + predicate;
        }
        checkArgument(key.operation() == UPDATE_OPERATION_NUMBER, "Unknown MERGE operation: %s", key.operation());
        List<Integer> channels = updateChannels.get(key.updateCase());
        checkArgument(channels != null && !channels.isEmpty(), "Unknown or empty MERGE update case: %s", key.updateCase());
        String assignments = channels.stream()
                .map(index -> client.quoted(columns.get(index).getColumnName()) + " = " + columnWriters.get(index).getBindExpression())
                .collect(joining(", "));
        return "UPDATE " + table + " SET " + assignments + " WHERE " + predicate;
    }

    private List<WriteFunction> writers(Connection connection, List<JdbcColumnHandle> handles)
    {
        return handles.stream()
                .map(column -> client.toColumnMapping(session, connection, column.getJdbcTypeHandle())
                        .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Unsupported YDB MERGE type: " + column.getJdbcTypeHandle()))
                        .getWriteFunction())
                .toList();
    }

    private static void bind(PreparedStatement statement, int index, Type type, WriteFunction writer, Block block, int position)
            throws SQLException
    {
        if (block.isNull(position)) {
            writer.setNull(statement, index);
        }
        else if (type.getJavaType() == boolean.class) {
            ((BooleanWriteFunction) writer).set(statement, index, type.getBoolean(block, position));
        }
        else if (type.getJavaType() == long.class) {
            ((LongWriteFunction) writer).set(statement, index, type.getLong(block, position));
        }
        else if (type.getJavaType() == double.class) {
            ((DoubleWriteFunction) writer).set(statement, index, type.getDouble(block, position));
        }
        else if (type.getJavaType() == Slice.class) {
            ((SliceWriteFunction) writer).set(statement, index, type.getSlice(block, position));
        }
        else {
            ((ObjectWriteFunction) writer).set(statement, index, type.getObject(block, position));
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        checkState(!finished, "MERGE sink is finished");
        finished = true;
        Slice fragment = Slices.allocate(Long.BYTES);
        fragment.setLong(0, pageSinkId.getId());
        return completedFuture(ImmutableList.of(fragment));
    }

    @Override
    public void abort()
    {
        // Completed batches are already committed in the explicitly non-transactional mode.
        finished = true;
    }

    private record StatementKey(int operation, int updateCase, Set<Integer> nullKeys) {}
}
