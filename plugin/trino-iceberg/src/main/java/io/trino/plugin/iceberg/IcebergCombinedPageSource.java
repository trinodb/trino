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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.EmptyPageSource;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

/**
 * Concatenates the Pages for all PageSources which were grouped together an Iceberg CombinedScanTask.
 */
public class IcebergCombinedPageSource
        implements ConnectorPageSource
{
    private final List<ConnectorPageSource> delegatePageSources;
    private final Iterator<ConnectorPageSource> pageSourceIterator;
    private final Closer closer = Closer.create();

    private ConnectorPageSource currentPageSource;
    private long completedBytes;
    private long readTimeNanos;

    public IcebergCombinedPageSource(List<ConnectorPageSource> delegatePageSources)
    {
        if (delegatePageSources.isEmpty()) {
            this.delegatePageSources = ImmutableList.of();
            this.pageSourceIterator = emptyIterator();
            this.currentPageSource = new EmptyPageSource();
        }
        else {
            this.delegatePageSources = ImmutableList.copyOf(requireNonNull(delegatePageSources, "delegatePageSources is null"));
            this.pageSourceIterator = delegatePageSources.iterator();
            this.currentPageSource = pageSourceIterator.next();
            this.delegatePageSources.forEach(closer::register);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes + currentPageSource.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegatePageSources.stream()
                .map(ConnectorPageSource::getCompletedPositions)
                .reduce(OptionalLong.of(0), (accumulatedPositions, newPositions) -> {
                    if (accumulatedPositions.isPresent() && newPositions.isPresent()) {
                        return OptionalLong.of(accumulatedPositions.getAsLong() + newPositions.getAsLong());
                    }
                    return OptionalLong.empty();
                });
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos + currentPageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return !pageSourceIterator.hasNext() && currentPageSource.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        if (currentPageSource.isFinished()) {
            completedBytes += currentPageSource.getReadTimeNanos();
            readTimeNanos += currentPageSource.getReadTimeNanos();
            try {
                currentPageSource = pageSourceIterator.next();
            }
            catch (NoSuchElementException e) {
                currentPageSource = new EmptyPageSource();
            }
        }

        return currentPageSource.getNextPage();
    }

    @Override
    public long getMemoryUsage()
    {
        return delegatePageSources.stream()
                .mapToLong(ConnectorPageSource::getMemoryUsage)
                .sum();
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return currentPageSource.isBlocked();
    }
}
