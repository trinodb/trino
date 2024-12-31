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

import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class DirectPaimonPageSource
        implements ConnectorPageSource
{
    private final Deque<ConnectorPageSource> pageSourceQueue;
    private ConnectorPageSource current;
    private long completedBytes;
    private long readTimeNanos;

    public DirectPaimonPageSource(List<ConnectorPageSource> pageSources)
    {
        this.pageSourceQueue = new ArrayDeque<>(requireNonNull(pageSources, "pageSources is null"));
        this.current = pageSourceQueue.poll();
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes + (current == null ? 0 : current.getCompletedBytes());
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos + (current == null ? 0 : current.getReadTimeNanos());
    }

    @Override
    public boolean isFinished()
    {
        return current == null || (current.isFinished() && pageSourceQueue.isEmpty());
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        while (current != null) {
            SourcePage dataPage = current.getNextSourcePage();
            if (dataPage != null) {
                return dataPage;
            }
            advance();
        }
        return null;
    }

    private void advance()
    {
        if (current == null) {
            throw new IllegalStateException("Current page source is null");
        }
        try {
            completedBytes += current.getCompletedBytes();
            readTimeNanos += current.getReadTimeNanos();
            current.close();
        }
        catch (IOException e) {
            current = null;
            close();
            throw new UncheckedIOException("Error closing page source", e);
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
