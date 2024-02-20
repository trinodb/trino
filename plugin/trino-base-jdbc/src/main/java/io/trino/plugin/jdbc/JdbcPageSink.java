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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.getWriteBatchSize;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class JdbcPageSink
        implements ConnectorPageSink
{
    private final Connection connection;
    private final PreparedStatement statement;

    private final List<Type> columnTypes;
    private final List<WriteFunction> columnWriters;
    private final int maxBatchSize;
    private int batchSize;

    private final ConnectorPageSinkId pageSinkId;
    private final LongWriteFunction pageSinkIdWriteFunction;
    private final boolean includePageSinkIdColumn;

    public JdbcPageSink(ConnectorSession session, JdbcOutputTableHandle handle, JdbcClient jdbcClient, ConnectorPageSinkId pageSinkId, RemoteQueryModifier remoteQueryModifier)
    {
        try {
            connection = jdbcClient.getConnection(session, handle);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }

        try {
            // According to JDBC javadocs "If a connection is in auto-commit mode, then all its SQL statements will be
            // executed and committed as individual transactions." Notably MySQL and SQL Server respect this which
            // leads to multiple commits when we close the connection leading to slow performance. Explicit commits
            // where needed to ensure that all the submitted statements are committed as a single transaction and
            // performs better.
            connection.setAutoCommit(false);
        }
        catch (SQLException e) {
            closeAllSuppress(e, connection);
            throw new TrinoException(JDBC_ERROR, e);
        }

        this.pageSinkId = pageSinkId;

        pageSinkIdWriteFunction = (LongWriteFunction) jdbcClient.toWriteMapping(session, BaseJdbcClient.TRINO_PAGE_SINK_ID_COLUMN_TYPE).getWriteFunction();
        includePageSinkIdColumn = handle.getPageSinkIdColumnName().isPresent();

        columnTypes = handle.getColumnTypes();

        if (handle.getJdbcColumnTypes().isEmpty()) {
            columnWriters = columnTypes.stream()
                    .map(type -> {
                        WriteMapping writeMapping = jdbcClient.toWriteMapping(session, type);
                        WriteFunction writeFunction = writeMapping.getWriteFunction();
                        verify(
                                type.getJavaType() == writeFunction.getJavaType(),
                                "Trino type %s is not compatible with write function %s accepting %s",
                                type,
                                writeFunction,
                                writeFunction.getJavaType());
                        return writeMapping;
                    })
                    .map(WriteMapping::getWriteFunction)
                    .collect(toImmutableList());
        }
        else {
            columnWriters = handle.getJdbcColumnTypes().get().stream()
                    .map(typeHandle -> jdbcClient.toColumnMapping(session, connection, typeHandle)
                            .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Underlying type is not supported for INSERT: " + typeHandle)))
                    .map(ColumnMapping::getWriteFunction)
                    .collect(toImmutableList());
        }

        String sinkSql = getSinkSql(jdbcClient, handle, columnWriters);
        try {
            sinkSql = remoteQueryModifier.apply(session, sinkSql);
            statement = connection.prepareStatement(sinkSql);
        }
        catch (TrinoException e) {
            throw closeAllSuppress(e, connection);
        }
        catch (SQLException e) {
            closeAllSuppress(e, connection);
            throw new TrinoException(JDBC_ERROR, e);
        }

        // Making batch size configurable allows performance tuning for insert/write-heavy workloads over multiple connections.
        this.maxBatchSize = getWriteBatchSize(session);
    }

    protected String getSinkSql(JdbcClient jdbcClient, JdbcOutputTableHandle outputTableHandle, List<WriteFunction> columnWriters)
    {
        return jdbcClient.buildInsertSql(outputTableHandle, columnWriters);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try {
            for (int position = 0; position < page.getPositionCount(); position++) {
                if (includePageSinkIdColumn) {
                    pageSinkIdWriteFunction.set(statement, page.getChannelCount() + 1, pageSinkId.getId());
                }

                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    appendColumn(page, position, channel);
                }

                statement.addBatch();
                batchSize++;

                if (batchSize >= maxBatchSize) {
                    statement.executeBatch();
                    connection.commit();
                    connection.setAutoCommit(false);
                    batchSize = 0;
                }
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        return NOT_BLOCKED;
    }

    private void appendColumn(Page page, int position, int channel)
            throws SQLException
    {
        Block block = page.getBlock(channel);
        int parameterIndex = channel + 1;

        WriteFunction writeFunction = columnWriters.get(channel);
        if (block.isNull(position)) {
            writeFunction.setNull(statement, parameterIndex);
            return;
        }

        Type type = columnTypes.get(channel);
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            ((BooleanWriteFunction) writeFunction).set(statement, parameterIndex, type.getBoolean(block, position));
        }
        else if (javaType == long.class) {
            ((LongWriteFunction) writeFunction).set(statement, parameterIndex, type.getLong(block, position));
        }
        else if (javaType == double.class) {
            ((DoubleWriteFunction) writeFunction).set(statement, parameterIndex, type.getDouble(block, position));
        }
        else if (javaType == Slice.class) {
            ((SliceWriteFunction) writeFunction).set(statement, parameterIndex, type.getSlice(block, position));
        }
        else {
            ((ObjectWriteFunction) writeFunction).set(statement, parameterIndex, type.getObject(block, position));
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // commit and close
        try (Connection connection = this.connection;
                PreparedStatement statement = this.statement) {
            if (batchSize > 0) {
                statement.executeBatch();
                connection.commit();
            }
        }
        catch (SQLNonTransientException e) {
            throw new TrinoException(JDBC_NON_TRANSIENT_ERROR, e);
        }
        catch (SQLException e) {
            // Convert chained SQLExceptions to suppressed exceptions, so they are visible in the stack trace
            SQLException nextException = e.getNextException();
            while (nextException != null) {
                if (e != nextException) {
                    e.addSuppressed(new Exception("Next SQLException", nextException));
                }
                nextException = nextException.getNextException();
            }
            throw new TrinoException(JDBC_ERROR, "Failed to insert data: " + firstNonNull(e.getMessage(), e), e);
        }
        // pass the successful page sink id
        Slice value = Slices.allocate(Long.BYTES);
        value.setLong(0, pageSinkId.getId());
        return completedFuture(ImmutableList.of(value));
    }

    @SuppressWarnings("unused")
    @Override
    public void abort()
    {
        // rollback and close
        try (Connection connection = this.connection;
                PreparedStatement statement = this.statement) {
            // skip rollback if implicitly closed due to an error
            if (!connection.isClosed()) {
                connection.rollback();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }
}
