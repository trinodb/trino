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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.bigquery.BigQueryClient.selectSql;
import static io.trino.plugin.bigquery.BigQueryType.toTrinoTimestamp;
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
    private static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 6, true)
            .optionalEnd()
            .toFormatter();

    private final List<Type> columnTypes;
    private final PageBuilder pageBuilder;
    private final TableResult tableResult;

    private boolean finished;

    public BigQueryQueryPageSource(
            ConnectorSession session,
            BigQueryClient client,
            BigQueryTableHandle table,
            List<String> columnNames,
            List<Type> columnTypes,
            Optional<String> filter)
    {
        requireNonNull(client, "client is null");
        requireNonNull(table, "table is null");
        requireNonNull(columnNames, "columnNames is null");
        requireNonNull(columnTypes, "columnTypes is null");
        requireNonNull(filter, "filter is null");
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes sizes don't match");
        this.columnTypes = ImmutableList.copyOf(columnTypes);
        this.pageBuilder = new PageBuilder(columnTypes);
        String sql = buildSql(table, client.getProjectId(), ImmutableList.copyOf(columnNames), filter);
        this.tableResult = client.executeQuery(session, sql);
    }

    private static String buildSql(BigQueryTableHandle table, String projectId, List<String> columnNames, Optional<String> filter)
    {
        if (table.getRelationHandle() instanceof BigQueryQueryRelationHandle queryRelationHandle) {
            if (filter.isEmpty()) {
                return queryRelationHandle.getQuery();
            }
            return "SELECT * FROM (" + queryRelationHandle.getQuery() + " ) WHERE " + filter.get();
        }
        TableId tableId = TableId.of(projectId, table.asPlainTable().getRemoteTableName().getDatasetName(), table.asPlainTable().getRemoteTableName().getTableName());
        return selectSql(tableId, ImmutableList.copyOf(columnNames), filter);
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
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
        for (FieldValueList record : tableResult.iterateAll()) {
            pageBuilder.declarePosition();
            for (int column = 0; column < columnTypes.size(); column++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(column);
                appendTo(columnTypes.get(column), record.get(column), output);
            }
        }
        finished = true;

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private void appendTo(Type type, FieldValue value, BlockBuilder output)
    {
        // TODO (https://github.com/trinodb/trino/issues/12346) Add support for bignumeric and timestamp with time zone types
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
                    type.writeLong(output, toTrinoTimestamp((value.getStringValue())));
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
            else if (javaType == Block.class) {
                writeBlock(output, type, value);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type), e);
        }
    }

    private static void writeSlice(BlockBuilder output, Type type, FieldValue value)
    {
        if (type instanceof VarcharType) {
            type.writeSlice(output, utf8Slice(value.getStringValue()));
        }
        else if (type instanceof VarbinaryType) {
            type.writeSlice(output, Slices.wrappedBuffer(value.getBytesValue()));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private void writeBlock(BlockBuilder output, Type type, FieldValue value)
    {
        if (type instanceof ArrayType) {
            BlockBuilder builder = output.beginBlockEntry();

            for (FieldValue element : value.getRepeatedValue()) {
                appendTo(type.getTypeParameters().get(0), element, builder);
            }

            output.closeEntry();
            return;
        }
        if (type instanceof RowType) {
            FieldValueList record = value.getRecordValue();
            BlockBuilder builder = output.beginBlockEntry();

            for (int index = 0; index < type.getTypeParameters().size(); index++) {
                appendTo(type.getTypeParameters().get(index), record.get(index), builder);
            }
            output.closeEntry();
            return;
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
    }

    @Override
    public void close() {}
}
