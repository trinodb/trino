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
package io.trino.plugin.base.classloader;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeUpdatablePageSource
        implements UpdatablePageSource
{
    private final UpdatablePageSource delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeUpdatablePageSource(UpdatablePageSource delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public long getCompletedBytes()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getCompletedBytes();
        }
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getCompletedPositions();
        }
    }

    @Override
    public long getReadTimeNanos()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getReadTimeNanos();
        }
    }

    @Override
    public boolean isFinished()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.isFinished();
        }
    }

    @Override
    public Page getNextPage()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getNextPage();
        }
    }

    @Override
    public long getMemoryUsage()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getMemoryUsage();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.close();
        }
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.isBlocked();
        }
    }

    @Override
    public Metrics getMetrics()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getMetrics();
        }
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.deleteRows(rowIds);
        }
    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.updateRows(page, columnValueAndRowIdChannels);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.finish();
        }
    }

    @Override
    public void abort()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.abort();
        }
    }
}
