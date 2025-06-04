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

import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorPageSink;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeConnectorPageSink
        implements ConnectorPageSink
{
    private final ConnectorPageSink delegate;
    private final ClassLoader classLoader;

    @Inject
    public ClassLoaderSafeConnectorPageSink(@ForClassLoaderSafe ConnectorPageSink delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public long getCompletedBytes()
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            return delegate.getCompletedBytes();
        }
    }

    @Override
    public long getMemoryUsage()
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            return delegate.getMemoryUsage();
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            return delegate.getValidationCpuNanos();
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            return delegate.appendPage(page);
        }
    }

    @Override
    public void closeIdleWriters()
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            delegate.closeIdleWriters();
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            return delegate.finish();
        }
    }

    @Override
    public void abort()
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            delegate.abort();
        }
    }
}
