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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.paimon.PaimonLongUtils.saturatedAdd;
import static java.util.Objects.requireNonNull;

public class PaimonPageSource
        implements ConnectorPageSource
{
    private static final int ROWS_PER_REQUEST = 4096;

    private final CloseableIterator<InternalRow> iterator;
    private final OptionalLong limit;
    private final PaimonPageBuilder pageBuilder;
    private final List<AutoCloseable> closeables;
    private final boolean[] closeablesClosed;

    private boolean isFinished;
    private boolean closed;
    private boolean iteratorClosed;
    private long numReturn;
    private long readTimeNanos;

    public PaimonPageSource(
            RecordReader<InternalRow> reader,
            List<? extends ColumnHandle> projectedColumns,
            OptionalLong limit)
    {
        this(reader, projectedColumns, limit, List.of());
    }

    PaimonPageSource(
            RecordReader<InternalRow> reader,
            List<? extends ColumnHandle> projectedColumns,
            OptionalLong limit,
            List<? extends AutoCloseable> closeables)
    {
        RecordReader<InternalRow> recordReader = requireNonNull(reader, "reader is null");
        List<AutoCloseable> extraCloseables = List.of();
        try {
            extraCloseables = copyCloseables(closeables);
            this.closeables = extraCloseables;
            this.closeablesClosed = new boolean[extraCloseables.size()];
            this.limit = requireNonNull(limit, "limit is null");
            checkArgument(this.limit.isEmpty() || this.limit.orElseThrow() >= 0, "limit must be non-negative");
            PageSourceColumns pageSourceColumns = pageSourceColumns(projectedColumns);
            this.pageBuilder = new PaimonPageBuilder(pageSourceColumns.columnTypes(), pageSourceColumns.logicalTypes());
        }
        catch (RuntimeException | Error e) {
            closeAllSuppress(e, recordReader);
            closeAllSuppress(e, extraCloseables.toArray(AutoCloseable[]::new));
            throw e;
        }

        try {
            // Paimon's RecordReaderIterator closes the reader if the initial readBatch fails.
            this.iterator = recordReader.toCloseableIterator();
        }
        catch (RuntimeException | Error e) {
            closeAllSuppress(e, extraCloseables.toArray(AutoCloseable[]::new));
            throw e;
        }
    }

    private static List<AutoCloseable> copyCloseables(List<? extends AutoCloseable> closeables)
    {
        requireNonNull(closeables, "closeables is null");
        ImmutableList.Builder<AutoCloseable> builder = ImmutableList.builder();
        for (AutoCloseable closeable : closeables) {
            builder.add(requireNonNull(closeable, "closeables contains null closeable"));
        }
        return builder.build();
    }

    private static PageSourceColumns pageSourceColumns(List<? extends ColumnHandle> projectedColumns)
    {
        List<Type> columnTypes = new ArrayList<>();
        List<DataType> logicalTypes = new ArrayList<>();
        requireNonNull(projectedColumns, "projectedColumns is null");
        for (ColumnHandle handle : projectedColumns) {
            if (!(requireNonNull(handle, "projectedColumns contains null column") instanceof PaimonColumnHandle paimonColumnHandle)) {
                throw new IllegalArgumentException("Paimon page source requires PaimonColumnHandle, got: "
                        + handle.getClass().getName());
            }
            columnTypes.add(paimonColumnHandle.getTrinoType());
            logicalTypes.add(paimonColumnHandle.logicalType());
        }
        return new PageSourceColumns(columnTypes, logicalTypes);
    }

    private record PageSourceColumns(List<Type> columnTypes, List<DataType> logicalTypes) {}

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(numReturn);
    }

    @Override
    public boolean isFinished()
    {
        return isFinished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        return ClassLoaderUtils.runWithContextClassLoader(() -> {
            long start = System.nanoTime();
            try {
                Page page = nextPage();
                return page == null ? null : SourcePage.create(page);
            }
            catch (TrinoException e) {
                closeAllSuppress(e, this);
                throw e;
            }
            catch (IOException e) {
                closeAllSuppress(e, this);
                throw PaimonPageSourceProvider.wrapPaimonReadException(e);
            }
            catch (UnsupportedOperationException e) {
                closeAllSuppress(e, this);
                throw PaimonPageSourceProvider.wrapPaimonReadException(e);
            }
            catch (RuntimeException e) {
                closeAllSuppress(e, this);
                throw PaimonPageSourceProvider.wrapPaimonReadException(e);
            }
            finally {
                readTimeNanos += System.nanoTime() - start;
            }
        }, PaimonPageSource.class.getClassLoader());
    }

    @Override
    public long getMemoryUsage()
    {
        return pageBuilder.getSizeInBytes();
    }

    @Nullable
    private Page nextPage()
            throws IOException
    {
        if (isFinished) {
            return null;
        }
        int count = 0;
        while (count < ROWS_PER_REQUEST && !pageBuilder.isFull()) {
            if (limit.isPresent() && count >= limit.orElseThrow() - numReturn) {
                return finishPage(count);
            }

            if (!iterator.hasNext()) {
                return finishPage(count);
            }

            InternalRow row = iterator.next();
            pageBuilder.appendRow(row);
            count++;
        }

        return returnPage(count);
    }

    @Nullable
    private Page finishPage(int count)
            throws IOException
    {
        isFinished = true;
        Page page = returnPage(count);
        close();
        return page;
    }

    private Page returnPage(int count)
    {
        if (count == 0) {
            return null;
        }
        numReturn = saturatedAdd(numReturn, count, "page position count");
        return pageBuilder.build();
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            ClassLoaderUtils.runWithContextClassLoader(() -> {
                try {
                    closeInternal();
                    return null;
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }, PaimonPageSource.class.getClassLoader());
        }
        catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private void closeInternal()
            throws IOException
    {
        if (closed) {
            return;
        }
        isFinished = true;
        Throwable failure = null;
        if (!iteratorClosed) {
            try {
                this.iterator.close();
                iteratorClosed = true;
            }
            catch (Throwable e) {
                failure = e;
            }
        }
        for (int i = 0; i < closeables.size(); i++) {
            if (closeablesClosed[i]) {
                continue;
            }
            try {
                closeables.get(i).close();
                closeablesClosed[i] = true;
            }
            catch (Throwable e) {
                if (failure == null) {
                    failure = e;
                }
                else if (failure != e) {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            if (failure instanceof IOException e) {
                throw e;
            }
            if (failure instanceof Error e) {
                throw e;
            }
            throw new IOException(failure);
        }
        closed = true;
    }
}
