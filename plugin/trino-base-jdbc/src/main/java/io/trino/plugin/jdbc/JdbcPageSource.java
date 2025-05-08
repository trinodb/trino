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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public final class JdbcPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(JdbcPageSource.class);

    private final List<JdbcColumnHandle> columnHandles;
    private final ReadFunction[] readFunctions;
    private final BooleanReadFunction[] booleanReadFunctions;
    private final DoubleReadFunction[] doubleReadFunctions;
    private final LongReadFunction[] longReadFunctions;
    private final SliceReadFunction[] sliceReadFunctions;
    private final ObjectReadFunction[] objectReadFunctions;

    private final JdbcClient jdbcClient;
    private final Connection connection;
    private final PreparedStatement statement;
    private final AtomicLong readTimeNanos = new AtomicLong(0);
    private final PageBuilder pageBuilder;
    private final CompletableFuture<ResultSet> resultSetFuture;
    @Nullable
    private ResultSet resultSet;
    private boolean finished;
    private boolean closed;
    private long completedPositions;

    public JdbcPageSource(JdbcClient jdbcClient, ExecutorService executor, ConnectorSession session, JdbcSplit split, BaseJdbcConnectorTableHandle table, List<JdbcColumnHandle> columnHandles)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.columnHandles = ImmutableList.copyOf(columnHandles);

        readFunctions = new ReadFunction[columnHandles.size()];
        booleanReadFunctions = new BooleanReadFunction[columnHandles.size()];
        doubleReadFunctions = new DoubleReadFunction[columnHandles.size()];
        longReadFunctions = new LongReadFunction[columnHandles.size()];
        sliceReadFunctions = new SliceReadFunction[columnHandles.size()];
        objectReadFunctions = new ObjectReadFunction[columnHandles.size()];

        try {
            if (table instanceof JdbcProcedureHandle procedureHandle) {
                connection = jdbcClient.getConnection(session, split, procedureHandle);
            }
            else {
                connection = jdbcClient.getConnection(session, split, (JdbcTableHandle) table);
            }

            for (int i = 0; i < this.columnHandles.size(); i++) {
                JdbcColumnHandle columnHandle = columnHandles.get(i);
                ColumnMapping columnMapping = jdbcClient.toColumnMapping(session, connection, columnHandle.getJdbcTypeHandle())
                        .orElseThrow(() -> new VerifyException("Column %s has unsupported type %s".formatted(columnHandle.getColumnName(), columnHandle.getJdbcTypeHandle())));
                verify(
                        columnHandle.getColumnType().equals(columnMapping.getType()),
                        "Type mismatch: column handle has type %s but %s is mapped to %s",
                        columnHandle.getColumnType(), columnHandle.getJdbcTypeHandle(), columnMapping.getType());
                Class<?> javaType = columnMapping.getType().getJavaType();
                ReadFunction readFunction = columnMapping.getReadFunction();
                readFunctions[i] = readFunction;

                if (javaType == boolean.class) {
                    booleanReadFunctions[i] = (BooleanReadFunction) readFunction;
                }
                else if (javaType == double.class) {
                    doubleReadFunctions[i] = (DoubleReadFunction) readFunction;
                }
                else if (javaType == long.class) {
                    longReadFunctions[i] = (LongReadFunction) readFunction;
                }
                else if (javaType == Slice.class) {
                    sliceReadFunctions[i] = (SliceReadFunction) readFunction;
                }
                else {
                    objectReadFunctions[i] = (ObjectReadFunction) readFunction;
                }
            }

            if (table instanceof JdbcProcedureHandle procedureHandle) {
                statement = jdbcClient.buildProcedure(session, connection, split, procedureHandle);
            }
            else {
                statement = jdbcClient.buildSql(session, connection, split, (JdbcTableHandle) table, columnHandles);
            }
            pageBuilder = new PageBuilder(columnHandles.stream()
                    .map(JdbcColumnHandle::getColumnType)
                    .collect(toImmutableList()));
            resultSetFuture = supplyAsync(() -> {
                long start = nanoTime();
                try {
                    log.debug("Executing: %s", statement);
                    return statement.executeQuery();
                }
                catch (SQLException e) {
                    throw handleSqlException(e);
                }
                finally {
                    readTimeNanos.addAndGet(nanoTime() - start);
                }
            }, executor);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos.get();
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        verify(pageBuilder.isEmpty(), "Expected pageBuilder to be empty");
        try {
            if (resultSet == null) {
                if (!resultSetFuture.isDone()) {
                    return null;
                }
                resultSet = requireNonNull(getFutureValue(resultSetFuture), "resultSet is null");
            }

            checkState(!closed, "page source is closed");
            while (!pageBuilder.isFull() && resultSet.next()) {
                pageBuilder.declarePosition();
                completedPositions++;
                for (int i = 0; i < columnHandles.size(); i++) {
                    BlockBuilder output = pageBuilder.getBlockBuilder(i);
                    Type type = columnHandles.get(i).getColumnType();
                    if (readFunctions[i].isNull(resultSet, i + 1)) {
                        output.appendNull();
                    }
                    else if (booleanReadFunctions[i] != null) {
                        type.writeBoolean(output, booleanReadFunctions[i].readBoolean(resultSet, i + 1));
                    }
                    else if (doubleReadFunctions[i] != null) {
                        type.writeDouble(output, doubleReadFunctions[i].readDouble(resultSet, i + 1));
                    }
                    else if (longReadFunctions[i] != null) {
                        type.writeLong(output, longReadFunctions[i].readLong(resultSet, i + 1));
                    }
                    else if (sliceReadFunctions[i] != null) {
                        type.writeSlice(output, sliceReadFunctions[i].readSlice(resultSet, i + 1));
                    }
                    else {
                        type.writeObject(output, objectReadFunctions[i].readObject(resultSet, i + 1));
                    }
                }
            }

            if (!pageBuilder.isFull()) {
                finished = true;
            }
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return SourcePage.create(page);
    }

    @Override
    public long getMemoryUsage()
    {
        return pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return resultSetFuture;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        // use try with resources to close everything properly
        try (Connection connection = this.connection;
                Statement statement = this.statement;
                ResultSet resultSet = this.resultSet) {
            if (statement != null) {
                try {
                    // Trying to cancel running statement as close() may not do it
                    statement.cancel();
                }
                catch (SQLException _) {
                    // statement already closed or cancel is not supported
                }
            }
            if (connection != null && resultSet != null) {
                jdbcClient.abortReadConnection(connection, resultSet);
                resultSetFuture.cancel(true);
            }
        }
        catch (SQLException | RuntimeException e) {
            // ignore exception from close
        }
        resultSet = null;
    }

    private RuntimeException handleSqlException(Exception e)
    {
        try {
            close();
        }
        catch (Exception closeException) {
            // Self-suppression not permitted
            if (e != closeException) {
                e.addSuppressed(closeException);
            }
        }
        return new TrinoException(JDBC_ERROR, e);
    }
}
