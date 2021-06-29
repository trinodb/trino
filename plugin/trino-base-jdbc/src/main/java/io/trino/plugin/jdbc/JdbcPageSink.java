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
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
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
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class JdbcPageSink
        implements ConnectorPageSink
{
    private final Connection connection;
    private final PreparedStatement statement;

    private final List<Type> columnTypes;
    private final List<WriteFunction> columnWriters;
    private int batchSize;

    public JdbcPageSink(ConnectorSession session, JdbcOutputTableHandle handle, JdbcClient jdbcClient)
    {
        try {
            connection = jdbcClient.getConnection(session, handle);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }

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

        try {
            // Per JDBC specification, auto-commit mode is the default. Verify that in case pooling or custom ConnectionFactory is used.
            verify(connection.getAutoCommit(), "Connection not in auto-commit");
            statement = connection.prepareStatement(jdbcClient.buildInsertSql(handle, columnWriters));
        }
        catch (SQLException e) {
            closeWithSuppression(connection, e);
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try {
            for (int position = 0; position < page.getPositionCount(); position++) {
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    appendColumn(page, position, channel);
                }

                statement.addBatch();
                batchSize++;

                if (batchSize >= 1000) {
                    statement.executeBatch();
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
            }
        }
        catch (SQLNonTransientException e) {
            throw new TrinoException(JDBC_NON_TRANSIENT_ERROR, e);
        }
        catch (SQLException e) {
            // Convert chained SQLExceptions to suppressed exceptions so they are visible in the stack trace
            SQLException nextException = e.getNextException();
            while (nextException != null) {
                if (e != nextException) {
                    e.addSuppressed(new Exception("Next SQLException", nextException));
                }
                nextException = nextException.getNextException();
            }
            throw new TrinoException(JDBC_ERROR, "Failed to insert data: " + firstNonNull(e.getMessage(), e), e);
        }
        // the committer does not need any additional info
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        // close statement and connection
        try (connection) {
            statement.close();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @SuppressWarnings("ObjectEquality")
    private static void closeWithSuppression(Connection connection, Throwable throwable)
    {
        try {
            connection.close();
        }
        catch (Throwable t) {
            // Self-suppression not permitted
            if (throwable != t) {
                throwable.addSuppressed(t);
            }
        }
    }
}
