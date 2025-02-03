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
package io.trino.plugin.redshift;

import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.reader.ParquetReader;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.OptionalLong;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.redshift.RedshiftErrorCode.REDSHIFT_PARQUET_BAD_DATA;
import static io.trino.plugin.redshift.RedshiftErrorCode.REDSHIFT_PARQUET_CURSOR_ERROR;
import static java.util.Objects.requireNonNull;

public class RedshiftParquetPageSource
        implements ConnectorPageSource
{
    private final ParquetReader parquetReader;
    private boolean closed;
    private long completedPositions;

    public RedshiftParquetPageSource(ParquetReader parquetReader)
    {
        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return parquetReader.getDataSource().getReadBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return parquetReader.getDataSource().getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        Page page;
        try {
            page = parquetReader.nextPage();
        }
        catch (IOException | RuntimeException e) {
            closeAllSuppress(e, this);
            throw handleException(e);
        }

        if (closed || page == null) {
            close();
            return null;
        }

        completedPositions += page.getPositionCount();
        return page;
    }

    @Override
    public long getMemoryUsage()
    {
        return parquetReader.getMemoryContext().getBytes();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            parquetReader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Metrics getMetrics()
    {
        return parquetReader.getMetrics();
    }

    static TrinoException handleException(Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(REDSHIFT_PARQUET_BAD_DATA, exception);
        }
        return new TrinoException(REDSHIFT_PARQUET_CURSOR_ERROR, exception.getMessage(), exception);
    }
}
