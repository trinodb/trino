package io.prestosql.plugin.bigquery;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.connector.ConnectorPageSource;

import java.io.IOException;

public class BigQueryEmptyProjectionPageSource
        implements ConnectorPageSource
{
    private final long numberOfRows;
    private boolean finished;

    public BigQueryEmptyProjectionPageSource(long numberOfRows)
    {
        this.numberOfRows = numberOfRows;
        this.finished = false;
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
    public Page getNextPage()
    {
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of());
        for (long i = 0; i < numberOfRows; i++) {
            pageBuilder.declarePosition();
        }
        finished = true;
        return pageBuilder.build();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        // nothing to do
    }
}
