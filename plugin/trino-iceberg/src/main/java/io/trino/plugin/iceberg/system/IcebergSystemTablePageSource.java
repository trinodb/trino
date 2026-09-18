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
package io.trino.plugin.iceberg.system;

import com.google.common.io.Closer;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public final class IcebergSystemTablePageSource
        implements ConnectorPageSource
{
    private final List<Type> types;
    private final TimeZoneKey timeZoneKey;
    private final RowWriter rowWriter;
    private final Map<String, Integer> columnNameToPosition;
    private final PageBuilder pageBuilder;
    private final Closer closer = Closer.create();
    private final Iterator<FileScanTask> fileScanTasksIterator;

    private boolean closed;
    private boolean finished;
    private int currentChannel;

    public IcebergSystemTablePageSource(
            List<Type> types,
            TimeZoneKey timeZoneKey,
            RowWriter rowWriter,
            Map<String, Integer> columnNameToPosition,
            TableScan tableScan)
    {
        this.types = requireNonNull(types, "types is null");
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.rowWriter = requireNonNull(rowWriter, "rowWriter is null");
        this.columnNameToPosition = requireNonNull(columnNameToPosition, "columnNameToPosition is null");
        this.pageBuilder = new PageBuilder(types);
        CloseableIterable<FileScanTask> fileScanTasks = closer.register(tableScan.planFiles());
        this.fileScanTasksIterator = closer.register(fileScanTasks.iterator());
        this.currentChannel = -1;
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
    public SourcePage getNextSourcePage()
    {
        verify(pageBuilder.isEmpty(), "Expected pageBuilder to be empty");
        if (finished) {
            return null;
        }

        checkState(!closed, "page source is closed");
        while (!pageBuilder.isFull() && fileScanTasksIterator.hasNext()) {
            try (CloseableIterable<StructLike> dataRows = fileScanTasksIterator.next().asDataTask().rows()) {
                dataRows.forEach(row -> {
                    BaseSystemTable.Row rowWrapper = new BaseSystemTable.Row(row, columnNameToPosition);
                    writeRow(rowWrapper);
                });
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        if (!fileScanTasksIterator.hasNext()) {
            finished = true;
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
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void appendInteger(Integer value)
    {
        if (checkNonNull(value)) {
            INTEGER.writeLong(nextColumn(), value);
        }
    }

    public void appendBigint(Long value)
    {
        if (checkNonNull(value)) {
            BIGINT.writeLong(nextColumn(), value);
        }
    }

    public void appendTimestampTzMillis(long millisUtc, TimeZoneKey timeZoneKey)
    {
        TIMESTAMP_TZ_MILLIS.writeLong(nextColumn(), packDateTimeWithZone(millisUtc, timeZoneKey));
    }

    public void appendVarchar(String value)
    {
        VARCHAR.writeString(nextColumn(), value);
    }

    public void appendVarcharVarcharMap(Map<String, String> values)
    {
        MapBlockBuilder column = (MapBlockBuilder) nextColumn();
        column.buildEntry((keyBuilder, valueBuilder) -> values.forEach((key, value) -> {
            VARCHAR.writeString(keyBuilder, key);
            VARCHAR.writeString(valueBuilder, value);
        }));
    }

    public BlockBuilder nextColumn()
    {
        int currentChannelValue = currentChannel;
        currentChannel++;
        return pageBuilder.getBlockBuilder(currentChannelValue);
    }

    @FunctionalInterface
    public interface RowWriter
    {
        void writeRow(IcebergSystemTablePageSource pageSource, BaseSystemTable.Row row, TimeZoneKey timeZoneKey);
    }

    private boolean checkNonNull(Object object)
    {
        if (object == null) {
            nextColumn().appendNull();
            return false;
        }
        return true;
    }

    private void writeRow(BaseSystemTable.Row row)
    {
        beginRow();
        rowWriter.writeRow(this, row, timeZoneKey);
        endRow();
    }

    private void beginRow()
    {
        checkArgument(currentChannel == -1, "already in row");
        pageBuilder.declarePosition();
        currentChannel = 0;
    }

    private void endRow()
    {
        checkArgument(currentChannel == types.size(), "not at end of row");
        currentChannel = -1;
    }
}
