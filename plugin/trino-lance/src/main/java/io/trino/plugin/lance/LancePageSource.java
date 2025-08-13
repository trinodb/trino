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
package io.trino.plugin.lance;

import io.trino.lance.file.LanceDataSource;
import io.trino.lance.file.LanceReader;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.io.IOException;
import java.io.UncheckedIOException;

import static java.util.Objects.requireNonNull;

public class LancePageSource
        implements ConnectorPageSource
{
    private final LanceReader reader;
    private final LanceDataSource dataSource;
    private final AggregatedMemoryContext memoryContext;

    private boolean closed;

    public LancePageSource(
            LanceReader reader,
            LanceDataSource dataSource,
            AggregatedMemoryContext memoryContext)
    {
        this.reader = requireNonNull(reader, "reader is null");
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return dataSource.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return dataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        SourcePage page = reader.nextSourcePage();
        if (closed || page == null) {
            close();
            return null;
        }
        return page;
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryContext.getBytes();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            reader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
