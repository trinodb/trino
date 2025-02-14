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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.bigquery.BigQueryClient.selectSql;
import static io.trino.plugin.bigquery.BigQueryTypeManager.toTrinoTimestamp;
import static io.trino.plugin.bigquery.BigQueryUtil.buildNativeQuery;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static java.lang.String.format;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.util.Objects.requireNonNull;

public class BigQueryQueryPageSource
        implements ConnectorPageSource
{
    private static final long MAX_PAGE_ROW_COUNT = 8192L;
    private static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 6, true)
            .optionalEnd()
            .toFormatter();

    private final BigQueryTypeManager typeManager;
    private final List<BigQueryColumnHandle> columnHandles;
    private final PageBuilder pageBuilder;
    private final boolean isQueryFunction;

    private final AtomicLong readTimeNanos = new AtomicLong();

    private CompletableFuture<TableResult> tableResultFuture;
    private TableResult tableResult;
    private boolean finished;

    public BigQueryQueryPageSource(
            ConnectorSession session,
            BigQueryTypeManager typeManager,
            BigQueryClient client,
            ExecutorService executor,
            BigQueryTableHandle table,
            List<BigQueryColumnHandle> columnHandles,
            Optional<String> filter)
    {
        requireNonNull(session, "session is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        requireNonNull(client, "client is null");
        requireNonNull(executor, "executor is null");
        requireNonNull(table, "table is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        requireNonNull(filter, "filter is null");
        this.pageBuilder = new PageBuilder(columnHandles.stream().map(BigQueryColumnHandle::trinoType).collect(toImmutableList()));
        this.isQueryFunction = table.relationHandle() instanceof BigQueryQueryRelationHandle;
        String sql = buildSql(
                table,
                client.getProjectId(),
                ImmutableList.copyOf(columnHandles),
                filter);
        this.tableResultFuture = CompletableFuture.supplyAsync(() -> {
            long start = System.nanoTime();
            TableResult result = client.executeQuery(session, sql, MAX_PAGE_ROW_COUNT);
            readTimeNanos.addAndGet(System.nanoTime() - start);
            return result;
        }, executor);
    }

    private String buildSql(BigQueryTableHandle table, String projectId, List<BigQueryColumnHandle> columns, Optional<String> filter)
    {
        if (isQueryFunction) {
            BigQueryQueryRelationHandle queryRelationHandle = (BigQueryQueryRelationHandle) table.relationHandle();
            return buildNativeQuery(queryRelationHandle.getQuery(), filter, table.limit());
        }
        TableId tableId = TableId.of(projectId, table.asPlainTable().getRemoteTableName().datasetName(), table.asPlainTable().getRemoteTableName().tableName());
        return selectSql(tableId, ImmutableList.copyOf(columns), filter, table.limit());
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
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
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public Page getNextPage()
    {
        verify(pageBuilder.isEmpty());
        if (tableResult == null) {
            tableResult = getFutureValue(tableResultFuture);
        }
        else if (tableResult.hasNextPage()) {
            long start = System.nanoTime();
            tableResult = tableResult.getNextPage();
            readTimeNanos.addAndGet(System.nanoTime() - start);
        }
        else {
            finished = true;
            return null;
        }

        long start = System.nanoTime();
        List<FieldValueList> values = ImmutableList.copyOf(tableResult.getValues());
        finished = !tableResult.hasNextPage();
        readTimeNanos.addAndGet(System.nanoTime() - start);

        for (FieldValueList record : values) {
            pageBuilder.declarePosition();
            for (int column = 0; column < columnHandles.size(); column++) {
                BigQueryColumnHandle columnHandle = columnHandles.get(column);
                BlockBuilder output = pageBuilder.getBlockBuilder(column);
                FieldValue fieldValue = isQueryFunction ? record.get(columnHandle.name()) : record.get(column);
                appendTo(columnHandle.trinoType(), fieldValue, output);
            }
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private void appendTo(Type type, FieldValue value, BlockBuilder output)
    {
        // TODO (https://github.com/trinodb/trino/issues/12346) Add support for timestamp with time zone type
        if (value == null || value.isNull()) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                type.writeBoolean(output, value.getBooleanValue());
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    type.writeLong(output, value.getLongValue());
                }
                else if (type.equals(INTEGER)) {
                    type.writeLong(output, value.getLongValue());
                }
                else if (type.equals(DATE)) {
                    type.writeLong(output, LocalDate.parse(value.getStringValue()).toEpochDay());
                }
                else if (type.equals(TIME_MICROS)) {
                    LocalTime time = LocalTime.parse(value.getStringValue(), TIME_FORMATTER);
                    long nanosOfDay = time.toNanoOfDay();
                    verify(nanosOfDay < NANOSECONDS_PER_DAY, "Invalid value of nanosOfDay: %s", nanosOfDay);
                    long picosOfDay = nanosOfDay * PICOSECONDS_PER_NANOSECOND;
                    long rounded = round(picosOfDay, 12 - TIME_MICROS.getPrecision());
                    if (rounded == PICOSECONDS_PER_DAY) {
                        rounded = 0;
                    }
                    type.writeLong(output, rounded);
                }
                else if (type.equals(TIMESTAMP_MICROS)) {
                    type.writeLong(output, toTrinoTimestamp(value.getStringValue()));
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (javaType == double.class) {
                type.writeDouble(output, value.getDoubleValue());
            }
            else if (type.getJavaType() == Int128.class) {
                DecimalType decimalType = (DecimalType) type;
                verify(!decimalType.isShort(), "The type should be long decimal");
                BigDecimal decimal = value.getNumericValue();
                type.writeObject(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
            }
            else if (javaType == Slice.class) {
                writeSlice(output, type, value);
            }
            else if (type instanceof ArrayType arrayType) {
                ((ArrayBlockBuilder) output).buildEntry(elementBuilder -> {
                    Type elementType = arrayType.getElementType();
                    for (FieldValue element : value.getRepeatedValue()) {
                        appendTo(elementType, element, elementBuilder);
                    }
                });
            }
            else if (type instanceof RowType rowType) {
                FieldValueList record = value.getRecordValue();
                List<RowType.Field> fields = rowType.getFields();
                ((RowBlockBuilder) output).buildEntry(fieldBuilders -> {
                    for (int index = 0; index < fields.size(); index++) {
                        appendTo(fields.get(index).getType(), record.get(index), fieldBuilders.get(index));
                    }
                });
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type), e);
        }
    }

    private void writeSlice(BlockBuilder output, Type type, FieldValue value)
    {
        if (type instanceof VarcharType || typeManager.isJsonType(type)) {
            type.writeSlice(output, utf8Slice(value.getStringValue()));
        }
        else if (type instanceof VarbinaryType) {
            type.writeSlice(output, Slices.wrappedBuffer(value.getBytesValue()));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    @Override
    public void close()
    {
        tableResultFuture.cancel(true);
        tableResult = null;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return tableResultFuture;
    }
}
