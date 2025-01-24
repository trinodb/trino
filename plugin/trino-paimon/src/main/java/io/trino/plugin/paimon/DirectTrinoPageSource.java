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

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedList;
import java.util.OptionalLong;

/**
 * Trino {@link ConnectorPageSource}.
 */
public class DirectTrinoPageSource
        implements ConnectorPageSource
{
    private final LinkedList<ConnectorPageSource> pageSourceQueue;
    private final OptionalLong limit;
    private ConnectorPageSource current;
    private long completedBytes;
    private long numReturn;

    public DirectTrinoPageSource(
            LinkedList<ConnectorPageSource> pageSourceQueue, OptionalLong limit)
    {
        this.pageSourceQueue = pageSourceQueue;
        this.current = pageSourceQueue.poll();
        this.limit = limit;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return System.nanoTime();
    }

    @Override
    public boolean isFinished()
    {
        return current == null || (current.isFinished() && pageSourceQueue.isEmpty());
    }

    @Override
    public Page getNextPage()
    {
        try {
            if (current == null) {
                return null;
            }
            if (limit.isPresent() && numReturn >= limit.getAsLong()) {
                return null;
            }
            Page dataPage = current.getNextPage();
            if (dataPage == null) {
                advance();
                return getNextPage();
            }

            numReturn += dataPage.getPositionCount();
            return dataPage;
        }
        catch (RuntimeException e) {
            throw new RuntimeException(e);
        }
    }

    private void advance()
    {
        if (current == null) {
            throw new RuntimeException("Current is null, should not invoke advance");
        }
        try {
            completedBytes += current.getCompletedBytes();
            current.close();
        }
        catch (IOException e) {
            current = null;
            close();
            throw new RuntimeException("error happens while advance and close old page source.");
        }
        current = pageSourceQueue.poll();
    }

    @Override
    public void close()
    {
        try {
            if (current != null) {
                current.close();
            }
            for (ConnectorPageSource source : pageSourceQueue) {
                source.close();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return current == null ? null : current.toString();
    }

    @Override
    public long getMemoryUsage()
    {
        return current == null ? 0 : current.getMemoryUsage();
    }

    @Override
    public Metrics getMetrics()
    {
        return current == null ? Metrics.EMPTY : current.getMetrics();
    }
}
