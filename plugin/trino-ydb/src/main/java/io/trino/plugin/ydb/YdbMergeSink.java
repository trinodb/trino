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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_OPERATION_NUMBER;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.joining;

final class YdbMergeSink
        implements ConnectorMergeSink
{
    private final Connection connection;
    private final ConnectorPageSinkId pageSinkId;
    private final List<JdbcColumnHandle> columns;
    private final List<JdbcColumnHandle> primaryKeys;
    private final List<WriteFunction> columnWriters;
    private final List<WriteFunction> keyWriters;
    private final List<PreparedStatement> statements = new ArrayList<>();
    private final PreparedStatement insert;
    private final PreparedStatement delete;
    private final Map<Integer, PreparedStatement> updates = new HashMap<>();
    private final Map<Integer, List<Integer>> updateChannels = new HashMap<>();
    private boolean closed;

    YdbMergeSink(
            ConnectorSession session,
            JdbcMergeTableHandle handle,
            JdbcClient client,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier modifier)
    {
        this.pageSinkId = pageSinkId;
        columns = handle.getDataColumns();
        primaryKeys = handle.getPrimaryKeys();
        try {
            connection = client.getConnection(session, handle.getOutputTableHandle());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to open YDB MERGE transaction", e);
        }
        try {
            // This sink owns the connection and commits all operation kinds together, never individual batches.
            connection.setAutoCommit(false);
            columnWriters = writers(session, client, columns);
            keyWriters = writers(session, client, primaryKeys);
            String table = client.quoted(handle.getOutputTableHandle().getRemoteTableName());
            insert = prepare(modifier.apply(session, client.buildInsertSql(handle.getOutputTableHandle(), columnWriters)));
            String predicate = IntStream.range(0, primaryKeys.size())
                    .mapToObj(index -> client.quoted(primaryKeys.get(index).getColumnName()) +
                            " IS NOT DISTINCT FROM " + keyWriters.get(index).getBindExpression())
                    .collect(joining(" AND "));
            delete = prepare(modifier.apply(session, "DELETE FROM " + table + " WHERE " + predicate));
            handle.getUpdateCaseColumns().forEach((caseNumber, updatedColumns) -> {
                List<Integer> channels = IntStream.range(0, columns.size())
                        .filter(index -> updatedColumns.contains(columns.get(index)))
                        .boxed()
                        .toList();
                checkArgument(!channels.isEmpty(), "Empty MERGE update case");
                updateChannels.put(caseNumber, channels);
                String assignments = channels.stream()
                        .map(index -> client.quoted(columns.get(index).getColumnName()) + " = " + columnWriters.get(index).getBindExpression())
                        .collect(joining(", "));
                try {
                    updates.put(caseNumber, prepare(modifier.apply(session, "UPDATE " + table + " SET " + assignments + " WHERE " + predicate)));
                }
                catch (SQLException e) {
                    throw new TrinoException(JDBC_ERROR, "Failed to prepare YDB MERGE update", e);
                }
            });
        }
        catch (SQLException e) {
            rollbackAndClose(e);
            throw new TrinoException(JDBC_ERROR, "Failed to prepare YDB MERGE transaction", e);
        }
        catch (RuntimeException e) {
            rollbackAndClose(e);
            throw e;
        }
    }

    private List<WriteFunction> writers(ConnectorSession session, JdbcClient client, List<JdbcColumnHandle> handles)
    {
        return handles.stream()
                .map(column -> client.toColumnMapping(session, connection, column.getJdbcTypeHandle())
                        .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Unsupported YDB MERGE type: " + column.getJdbcTypeHandle()))
                        .getWriteFunction())
                .toList();
    }

    private PreparedStatement prepare(String sql)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        statements.add(statement);
        return statement;
    }

    @Override
    public void storeMergedRows(Page page)
    {
        checkState(!closed, "MERGE sink is closed");
        checkArgument(page.getChannelCount() == columns.size() + 3, "Invalid MERGE page channel count");
        List<Block> rowIdFields = RowBlock.getRowFieldsFromBlock(page.getBlock(columns.size() + 2));
        try {
            for (int position = 0; position < page.getPositionCount(); position++) {
                int operation = TINYINT.getByte(page.getBlock(columns.size()), position);
                switch (operation) {
                    case INSERT_OPERATION_NUMBER -> {
                        for (int channel = 0; channel < columns.size(); channel++) {
                            bind(insert, channel + 1, columns.get(channel).getColumnType(), columnWriters.get(channel), page.getBlock(channel), position);
                        }
                        insert.executeUpdate();
                    }
                    case DELETE_OPERATION_NUMBER -> {
                        bindKeys(delete, 1, rowIdFields, position);
                        delete.executeUpdate();
                    }
                    case UPDATE_OPERATION_NUMBER -> {
                        int caseNumber = INTEGER.getInt(page.getBlock(columns.size() + 1), position);
                        PreparedStatement statement = updates.get(caseNumber);
                        List<Integer> channels = updateChannels.get(caseNumber);
                        checkArgument(statement != null, "Unknown MERGE update case %s", caseNumber);
                        for (int index = 0; index < channels.size(); index++) {
                            int channel = channels.get(index);
                            bind(statement, index + 1, columns.get(channel).getColumnType(), columnWriters.get(channel), page.getBlock(channel), position);
                        }
                        bindKeys(statement, channels.size() + 1, rowIdFields, position);
                        statement.executeUpdate();
                    }
                    default -> throw new IllegalArgumentException("Unknown MERGE operation: " + operation);
                }
            }
        }
        catch (SQLException e) {
            rollbackAndClose(e);
            throw new TrinoException(JDBC_ERROR, "YDB MERGE transaction failed", e);
        }
        catch (RuntimeException e) {
            rollbackAndClose(e);
            throw e;
        }
    }

    private void bindKeys(PreparedStatement statement, int start, List<Block> fields, int position)
            throws SQLException
    {
        for (int index = 0; index < primaryKeys.size(); index++) {
            bind(statement, start + index, primaryKeys.get(index).getColumnType(), keyWriters.get(index), fields.get(index), position);
        }
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
        checkState(!closed, "MERGE sink is closed");
        try {
            connection.commit();
        }
        catch (SQLException e) {
            rollbackAndClose(e);
            throw new TrinoException(JDBC_ERROR, "YDB MERGE commit failed", e);
        }
        closed = true;
        SQLException closeFailure = new SQLException("Failed to close YDB MERGE resources");
        closeAllSuppress(closeFailure, statements.toArray(PreparedStatement[]::new));
        closeAllSuppress(closeFailure, connection);
        if (closeFailure.getSuppressed().length > 0) {
            throw new TrinoException(JDBC_ERROR, closeFailure);
        }
        Slice fragment = Slices.allocate(Long.BYTES);
        fragment.setLong(0, pageSinkId.getId());
        return completedFuture(ImmutableList.of(fragment));
    }

    @Override
    public void abort()
    {
        if (closed) {
            return;
        }
        SQLException failure = new SQLException("Failed to abort YDB MERGE transaction");
        rollbackAndClose(failure);
        if (failure.getSuppressed().length > 0) {
            throw new TrinoException(JDBC_ERROR, failure);
        }
    }

    private void rollbackAndClose(Throwable failure)
    {
        closed = true;
        try {
            connection.rollback();
        }
        catch (SQLException e) {
            failure.addSuppressed(e);
        }
        closeAllSuppress(failure, statements.toArray(PreparedStatement[]::new));
        closeAllSuppress(failure, connection);
    }
}
