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
package io.trino.cli;

import com.google.common.collect.ImmutableList;
import io.trino.jdbc.QueryStats;
import io.trino.jdbc.Row;
import io.trino.jdbc.TrinoResultSet;
import io.trino.jdbc.TrinoStatement;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Types;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Executes a statement on a background thread, which is the only thread using the statement
 * and its result set, apart from cancelling it. Rows are passed to the consumer through a bounded queue.
 * Trino specific features, like progress tracking, are only used when the driver supports them.
 */
class StatementExecution
{
    static final List<?> END_OF_ROWS = new ArrayList<>(0);

    private static final long ENQUEUE_TIMEOUT_MILLIS = 100;

    private final Statement statement;
    private final String sql;
    private final Optional<TrinoStatement> trinoStatement;
    private final BlockingQueue<List<?>> rows;
    private final AtomicReference<QueryStats> stats = new AtomicReference<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final CountDownLatch resultsAvailable = new CountDownLatch(1);
    private final CountDownLatch finished = new CountDownLatch(1);
    private volatile boolean cancelled;

    // published before resultsAvailable is released
    private volatile Optional<List<Column>> columns = Optional.empty();
    private volatile Optional<String> updateStatus = Optional.empty();

    // published before finished is released
    private volatile Optional<Throwable> failure = Optional.empty();
    private volatile List<SQLWarning> finalWarnings = ImmutableList.of();

    public StatementExecution(Statement statement, String sql, int maxQueuedRows)
    {
        this.statement = requireNonNull(statement, "statement is null");
        this.sql = requireNonNull(sql, "sql is null");
        this.rows = new ArrayBlockingQueue<>(maxQueuedRows);
        this.trinoStatement = unwrap(statement, TrinoStatement.class);
        trinoStatement.ifPresent(trino -> trino.setProgressMonitor(stats::set));
    }

    public void start(Executor executor)
    {
        checkState(started.compareAndSet(false, true), "Execution already started");
        executor.execute(this::execute);
    }

    public boolean isStarted()
    {
        return started.get();
    }

    /**
     * Waits until the first row, the update count, or a failure is available.
     */
    public boolean awaitResults(long timeout, TimeUnit unit)
            throws InterruptedException
    {
        return resultsAvailable.await(timeout, unit);
    }

    public void awaitFinished()
            throws InterruptedException
    {
        finished.await();
    }

    public boolean awaitFinishedUninterruptibly(long timeout, TimeUnit unit)
    {
        return awaitUninterruptibly(finished, timeout, unit);
    }

    public boolean isFinished()
    {
        return finished.getCount() == 0;
    }

    public Optional<List<Column>> getColumns()
    {
        return columns;
    }

    public Optional<String> getUpdateStatus()
    {
        return updateStatus;
    }

    /**
     * Rows of the result set, followed by {@link #END_OF_ROWS}, unless the execution was cancelled.
     */
    public BlockingQueue<List<?>> getRows()
    {
        return rows;
    }

    public Optional<Throwable> getFailure()
    {
        return failure;
    }

    /**
     * Returns the latest progress, if the driver reports it.
     */
    public Optional<QueryStats> getStats()
    {
        return Optional.ofNullable(stats.get());
    }

    /**
     * Returns warnings reported so far. Drivers other than Trino are not required to be thread safe,
     * so their warnings are only available after the execution finished.
     */
    public List<SQLWarning> getWarnings()
    {
        if (isFinished()) {
            return finalWarnings;
        }
        if (trinoStatement.isEmpty()) {
            return ImmutableList.of();
        }
        try {
            return toList(trinoStatement.get().getWarnings());
        }
        catch (SQLException e) {
            return ImmutableList.of();
        }
    }

    public void partialCancel()
    {
        if (trinoStatement.isEmpty()) {
            return;
        }
        try {
            trinoStatement.get().partialCancel();
        }
        catch (SQLException ignored) {
        }
    }

    public void cancel()
    {
        cancelled = true;
        try {
            statement.cancel();
        }
        catch (SQLException | RuntimeException ignored) {
            // the statement may already be closed
        }
    }

    private void execute()
    {
        ResultSet resultSet = null;
        try {
            if (!statement.execute(sql)) {
                updateStatus = Optional.of(readUpdateStatus());
                // updates can also return rows, like EXPLAIN ANALYZE of an INSERT
                if (!hasMoreResults()) {
                    return;
                }
            }
            resultSet = statement.getResultSet();
            List<Column> resultColumns = readColumns(resultSet.getMetaData());
            RowReader rowReader = createRowReader(resultSet, resultColumns);
            boolean hasRow = resultSet.next();
            columns = Optional.of(resultColumns);
            resultsAvailable.countDown();
            while (hasRow) {
                if (!enqueue(rowReader.read())) {
                    return;
                }
                hasRow = resultSet.next();
            }
        }
        catch (SQLException | RuntimeException | Error e) {
            failure = Optional.of(e);
        }
        finally {
            finalWarnings = collectWarnings(resultSet);
            closeQuietly(resultSet);
            enqueue(END_OF_ROWS);
            finished.countDown();
            resultsAvailable.countDown();
        }
    }

    private boolean enqueue(List<?> row)
    {
        try {
            while (!rows.offer(row, ENQUEUE_TIMEOUT_MILLIS, MILLISECONDS)) {
                if (cancelled) {
                    return false;
                }
            }
            return true;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cancelled = true;
            return false;
        }
    }

    private String readUpdateStatus()
            throws SQLException
    {
        if (trinoStatement.isPresent()) {
            return formatUpdateStatus(Optional.ofNullable(trinoStatement.get().getUpdateType()), trinoStatement.get().getReportedUpdateCount());
        }
        long updateCount;
        try {
            updateCount = statement.getLargeUpdateCount();
        }
        catch (UnsupportedOperationException e) {
            // drivers implementing JDBC versions older than 4.2
            updateCount = statement.getUpdateCount();
        }
        return formatUpdateStatus(Optional.empty(), updateCount >= 0 ? OptionalLong.of(updateCount) : OptionalLong.empty());
    }

    private boolean hasMoreResults()
    {
        try {
            return statement.getMoreResults();
        }
        catch (SQLException | RuntimeException e) {
            // drivers are not required to support multiple results
            return false;
        }
    }

    static String formatUpdateStatus(Optional<String> updateType, OptionalLong updateCount)
    {
        if (updateCount.isEmpty()) {
            return updateType.orElse("OK");
        }
        long count = updateCount.orElseThrow();
        String rowCount = format("%s row%s", count, (count != 1) ? "s" : "");
        return updateType.map(type -> type + ": " + rowCount).orElse(rowCount);
    }

    private List<SQLWarning> collectWarnings(ResultSet resultSet)
    {
        ImmutableList.Builder<SQLWarning> warnings = ImmutableList.builder();
        try {
            warnings.addAll(toList(statement.getWarnings()));
            if (resultSet != null) {
                warnings.addAll(toList(resultSet.getWarnings()));
            }
        }
        catch (SQLException | RuntimeException ignored) {
        }
        return warnings.build();
    }

    private static List<SQLWarning> toList(SQLWarning warning)
    {
        ImmutableList.Builder<SQLWarning> warnings = ImmutableList.builder();
        for (SQLWarning current = warning; current != null; current = current.getNextWarning()) {
            warnings.add(current);
        }
        return warnings.build();
    }

    private static List<Column> readColumns(ResultSetMetaData metadata)
            throws SQLException
    {
        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            columns.add(new Column(metadata.getColumnLabel(i), metadata.getColumnType(i)));
        }
        return columns.build();
    }

    private static RowReader createRowReader(ResultSet resultSet, List<Column> columns)
    {
        int columnCount = columns.size();
        Optional<TrinoResultSet> trinoResultSet = unwrap(resultSet, TrinoResultSet.class);
        if (trinoResultSet.isPresent()) {
            // use the values formatted by the server, which are more precise than JDBC types
            TrinoResultSet trino = trinoResultSet.get();
            return () -> {
                Object[] values = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    values[i] = trino.getRawObject(i + 1);
                }
                return Arrays.asList(values);
            };
        }

        int[] types = columns.stream()
                .mapToInt(Column::getJdbcType)
                .toArray();
        return () -> {
            Object[] values = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                values[i] = readValue(resultSet, i + 1, types[i]);
            }
            return Arrays.asList(values);
        };
    }

    private static Object readValue(ResultSet resultSet, int column, int type)
            throws SQLException
    {
        switch (type) {
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return resultSet.getBytes(column);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.SQLXML:
            case Types.DATE:
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                // drivers format temporal values without the precision loss of the java.sql types
                return resultSet.getString(column);
            default:
                return toDisplayValue(resultSet.getObject(column));
        }
    }

    private static Object toDisplayValue(Object value)
            throws SQLException
    {
        if (value instanceof Array) {
            Array array = (Array) value;
            try {
                return toDisplayValue(array.getArray());
            }
            finally {
                freeQuietly(array);
            }
        }
        if (value instanceof Object[]) {
            List<Object> elements = new ArrayList<>();
            for (Object element : (Object[]) value) {
                elements.add(toDisplayValue(element));
            }
            return elements;
        }
        if (value != null && value.getClass().isArray() && !(value instanceof byte[])) {
            List<Object> elements = new ArrayList<>();
            for (int i = 0; i < java.lang.reflect.Array.getLength(value); i++) {
                elements.add(java.lang.reflect.Array.get(value, i));
            }
            return elements;
        }
        if (value instanceof Struct) {
            Row.Builder row = Row.builder();
            for (Object attribute : ((Struct) value).getAttributes()) {
                row.addUnnamedField(toDisplayValue(attribute));
            }
            return row.build();
        }
        if (value instanceof Clob) {
            Clob clob = (Clob) value;
            return clob.getSubString(1, toIntExact(clob.length()));
        }
        if (value instanceof Blob) {
            Blob blob = (Blob) value;
            return blob.getBytes(1, toIntExact(blob.length()));
        }
        return value;
    }

    private static void freeQuietly(Array array)
    {
        try {
            array.free();
        }
        catch (SQLException | RuntimeException | AbstractMethodError ignored) {
        }
    }

    private static void closeQuietly(ResultSet resultSet)
    {
        if (resultSet == null) {
            return;
        }
        try {
            resultSet.close();
        }
        catch (SQLException | RuntimeException ignored) {
        }
    }

    private static <T> Optional<T> unwrap(Wrapper wrapper, Class<T> type)
    {
        try {
            if (wrapper.isWrapperFor(type)) {
                return Optional.of(wrapper.unwrap(type));
            }
        }
        catch (SQLException | RuntimeException | AbstractMethodError ignored) {
            // drivers are not required to support unwrapping
        }
        return Optional.empty();
    }

    private interface RowReader
    {
        List<?> read()
                throws SQLException;
    }
}
