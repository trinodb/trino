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
import io.trino.spi.type.Type;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;

public class JdbcPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(JdbcPageSource.class);

    private static final int ROWS_PER_REQUEST = 4096;
    private final PageBuilder pageBuilder;
    private boolean closed;

    private final JdbcColumnHandle[] columnHandles;
    private final ReadFunction[] readFunctions;
    private final BooleanReadFunction[] booleanReadFunctions;
    private final DoubleReadFunction[] doubleReadFunctions;
    private final LongReadFunction[] longReadFunctions;
    private final SliceReadFunction[] sliceReadFunctions;
    private final ObjectReadFunction[] objectReadFunctions;
    private final Type[] types;

    private final JdbcClient jdbcClient;
    private final Connection connection;
    private final PreparedStatement statement;
    @Nullable
    private ResultSet resultSet;

    public JdbcPageSource(JdbcClient jdbcClient, ConnectorSession session, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columnHandles)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");

        this.columnHandles = columnHandles.toArray(new JdbcColumnHandle[0]);

        int columnsCount = columnHandles.size();
        readFunctions = new ReadFunction[columnsCount];
        booleanReadFunctions = new BooleanReadFunction[columnsCount];
        doubleReadFunctions = new DoubleReadFunction[columnsCount];
        longReadFunctions = new LongReadFunction[columnsCount];
        sliceReadFunctions = new SliceReadFunction[columnsCount];
        objectReadFunctions = new ObjectReadFunction[columnsCount];
        types = new Type[columnsCount];

        try {
            connection = jdbcClient.getConnection(session, split);

            for (int i = 0; i < this.columnHandles.length; i++) {
                JdbcColumnHandle columnHandle = columnHandles.get(i);
                ColumnMapping columnMapping = jdbcClient.toColumnMapping(session, connection, columnHandle.getJdbcTypeHandle())
                        .orElseThrow(() -> new VerifyException("Unsupported column type"));
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
                types[i] = columnHandle.getColumnType();
            }
            this.pageBuilder = new PageBuilder(ImmutableList.copyOf(types));

            statement = jdbcClient.buildSql(session, connection, split, table, columnHandles);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public Page getNextPage()
    {
        if (!closed) {
            try {
                if (resultSet == null) {
                    log.debug("Executing: %s", statement.toString());
                    resultSet = statement.executeQuery();
                }
                for (int i = 0; i < ROWS_PER_REQUEST && !pageBuilder.isFull(); i++) {
                    if (!resultSet.next()) {
                        closed = true;
                        break;
                    }
                    readPosition();
                }
            }
            catch (SQLException | RuntimeException e) {
                throw handleSqlException(e);
            }
        }

        // only return a page if the buffer is full or we are finishing
        if ((closed && !pageBuilder.isEmpty()) || pageBuilder.isFull()) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        return null;
    }

    private void readPosition()
            throws SQLException
    {
        pageBuilder.declarePosition();
        for (int column = 0; column < types.length; column++) {
            BlockBuilder output = pageBuilder.getBlockBuilder(column);
            if (readFunctions[column].isNull(resultSet, column + 1)) {
                output.appendNull();
            }
            else {
                Type type = types[column];
                Class<?> javaType = type.getJavaType();
                if (javaType == boolean.class) {
                    type.writeBoolean(output, booleanReadFunctions[column].readBoolean(resultSet, column + 1));
                }
                else if (javaType == long.class) {
                    type.writeLong(output, longReadFunctions[column].readLong(resultSet, column + 1));
                }
                else if (javaType == double.class) {
                    type.writeDouble(output, doubleReadFunctions[column].readDouble(resultSet, column + 1));
                }
                else if (javaType == Slice.class) {
                    Slice slice = sliceReadFunctions[column].readSlice(resultSet, column + 1);
                    type.writeSlice(output, slice, 0, slice.length());
                }
                else {
                    type.writeObject(output, objectReadFunctions[column].readObject(resultSet, column + 1));
                }
            }
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return pageBuilder.getSizeInBytes();
    }

    @Override
    public boolean isFinished()
    {
        return closed && pageBuilder.isEmpty();
    }

    @SuppressWarnings("UnusedDeclaration")
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
                catch (SQLException ignored) {
                    // statement already closed or cancel is not supported
                }
            }
            if (connection != null) {
                jdbcClient.abortReadConnection(connection);
            }
        }
        catch (SQLException | RuntimeException e) {
            // ignore exception from close
        }
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
