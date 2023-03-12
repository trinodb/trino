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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.kudu.Schema;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KeyEncoderAccessor;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduOperationApplier;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.truncateEpochMicrosToMillis;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class KuduPageSink
        implements ConnectorPageSink, ConnectorMergeSink
{
    private final ConnectorSession connectorSession;
    private final KuduClientSession session;
    private final KuduTable table;
    private final List<Type> columnTypes;
    private final List<Type> originalColumnTypes;
    private final boolean generateUUID;

    private final String uuid;
    private int nextSubId;

    public KuduPageSink(
            ConnectorSession connectorSession,
            KuduClientSession clientSession,
            KuduInsertTableHandle tableHandle)
    {
        this(connectorSession, clientSession, tableHandle.getTable(clientSession), tableHandle);
    }

    public KuduPageSink(
            ConnectorSession connectorSession,
            KuduClientSession clientSession,
            KuduOutputTableHandle tableHandle)
    {
        this(connectorSession, clientSession, tableHandle.getTable(clientSession), tableHandle);
    }

    public KuduPageSink(
            ConnectorSession connectorSession,
            KuduClientSession clientSession,
            KuduMergeTableHandle tableHandle)
    {
        this(connectorSession, clientSession, tableHandle.getOutputTableHandle().getTable(clientSession), tableHandle);
    }

    private KuduPageSink(
            ConnectorSession connectorSession,
            KuduClientSession clientSession,
            KuduTable table,
            KuduTableMapping mapping)
    {
        requireNonNull(clientSession, "clientSession is null");
        this.connectorSession = connectorSession;
        this.columnTypes = mapping.getColumnTypes();
        this.originalColumnTypes = mapping.getOriginalColumnTypes();
        this.generateUUID = mapping.isGenerateUUID();

        this.table = table;
        this.session = clientSession;
        uuid = UUID.randomUUID().toString();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try (KuduOperationApplier operationApplier = KuduOperationApplier.fromKuduClientSession(session)) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                Upsert upsert = table.newUpsert();
                PartialRow row = upsert.getRow();
                int start = 0;
                if (generateUUID) {
                    String id = format("%s-%08x", uuid, nextSubId++);
                    row.addString(0, id);
                    start = 1;
                }

                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    appendColumn(row, page, position, channel, channel + start);
                }

                operationApplier.applyOperationAsync(upsert);
            }
            return NOT_BLOCKED;
        }
        catch (KuduException e) {
            throw new RuntimeException(e);
        }
    }

    private void appendColumn(PartialRow row, Page page, int position, int channel, int destChannel)
    {
        Block block = page.getBlock(channel);
        Type type = columnTypes.get(destChannel);
        if (block.isNull(position)) {
            row.setNull(destChannel);
        }
        else if (TIMESTAMP_MILLIS.equals(type)) {
            row.addLong(destChannel, truncateEpochMicrosToMillis(type.getLong(block, position)));
        }
        else if (REAL.equals(type)) {
            row.addFloat(destChannel, intBitsToFloat(toIntExact(type.getLong(block, position))));
        }
        else if (BIGINT.equals(type)) {
            row.addLong(destChannel, type.getLong(block, position));
        }
        else if (INTEGER.equals(type)) {
            row.addInt(destChannel, toIntExact(type.getLong(block, position)));
        }
        else if (SMALLINT.equals(type)) {
            row.addShort(destChannel, Shorts.checkedCast(type.getLong(block, position)));
        }
        else if (TINYINT.equals(type)) {
            row.addByte(destChannel, SignedBytes.checkedCast(type.getLong(block, position)));
        }
        else if (BOOLEAN.equals(type)) {
            row.addBoolean(destChannel, type.getBoolean(block, position));
        }
        else if (DOUBLE.equals(type)) {
            row.addDouble(destChannel, type.getDouble(block, position));
        }
        else if (type instanceof VarcharType) {
            Type originalType = originalColumnTypes.get(destChannel);
            if (DATE.equals(originalType)) {
                SqlDate date = (SqlDate) originalType.getObjectValue(connectorSession, block, position);
                LocalDateTime ldt = LocalDateTime.ofEpochSecond(TimeUnit.DAYS.toSeconds(date.getDays()), 0, ZoneOffset.UTC);
                byte[] bytes = ldt.format(DateTimeFormatter.ISO_LOCAL_DATE).getBytes(StandardCharsets.UTF_8);
                row.addStringUtf8(destChannel, bytes);
            }
            else {
                row.addString(destChannel, type.getSlice(block, position).toStringUtf8());
            }
        }
        else if (VARBINARY.equals(type)) {
            row.addBinary(destChannel, type.getSlice(block, position).toByteBuffer());
        }
        else if (type instanceof DecimalType) {
            SqlDecimal sqlDecimal = (SqlDecimal) type.getObjectValue(connectorSession, block, position);
            row.addDecimal(destChannel, sqlDecimal.toBigDecimal());
        }
        else {
            throw new UnsupportedOperationException("Type is not supported: " + type);
        }
    }

    @Override
    public void storeMergedRows(Page page)
    {
        // The last channel in the page is the rowId block, the next-to-last is the operation block
        int columnCount = columnTypes.size();
        checkArgument(page.getChannelCount() == 2 + columnCount, "The page size should be 2 + columnCount (%s), but is %s", columnCount, page.getChannelCount());
        Block operationBlock = page.getBlock(columnCount);
        Block rowIds = page.getBlock(columnCount + 1);

        Schema schema = table.getSchema();
        try (KuduOperationApplier operationApplier = KuduOperationApplier.fromKuduClientSession(session)) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                long operation = TINYINT.getLong(operationBlock, position);

                checkState(operation == UPDATE_OPERATION_NUMBER ||
                                operation == INSERT_OPERATION_NUMBER ||
                                operation == DELETE_OPERATION_NUMBER,
                        "Invalid operation value, supported are " +
                                        "%s INSERT_OPERATION_NUMBER, " +
                                        "%s DELETE_OPERATION_NUMBER and " +
                                        "%s UPDATE_OPERATION_NUMBER",
                                INSERT_OPERATION_NUMBER, DELETE_OPERATION_NUMBER, UPDATE_OPERATION_NUMBER);

                if (operation == DELETE_OPERATION_NUMBER || operation == UPDATE_OPERATION_NUMBER) {
                    Delete delete = table.newDelete();
                    Slice deleteRowId = VARBINARY.getSlice(rowIds, position);
                    RowHelper.copyPrimaryKey(schema, KeyEncoderAccessor.decodePrimaryKey(schema, deleteRowId.getBytes()), delete.getRow());
                    try {
                        operationApplier.applyOperationAsync(delete);
                    }
                    catch (KuduException e) {
                        throw new RuntimeException(e);
                    }
                }

                if (operation == INSERT_OPERATION_NUMBER || operation == UPDATE_OPERATION_NUMBER) {
                    Insert insert = table.newInsert();
                    PartialRow insertRow = insert.getRow();
                    int insertStart = 0;
                    if (generateUUID) {
                        String id = format("%s-%08x", uuid, nextSubId++);
                        insertRow.addString(0, id);
                        insertStart = 1;
                    }
                    for (int channel = 0; channel < columnCount; channel++) {
                        appendColumn(insertRow, page, position, channel, channel + insertStart);
                    }
                    try {
                        operationApplier.applyOperationAsync(insert);
                    }
                    catch (KuduException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        catch (KuduException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
    }
}
