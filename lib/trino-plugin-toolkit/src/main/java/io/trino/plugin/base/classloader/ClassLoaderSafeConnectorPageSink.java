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
import io.trino.spi.connector.ConnectorPageSink;

import javax.inject.Inject;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.classloader.ThreadContextClassLoader.withClassLoader;
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
        return withClassLoader(classLoader, delegate::getCompletedBytes);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return withClassLoader(classLoader, delegate::getSystemMemoryUsage);
    }

    @Override
    public long getValidationCpuNanos()
    {
        return withClassLoader(classLoader, delegate::getValidationCpuNanos);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        return withClassLoader(classLoader, () -> delegate.appendPage(page));
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return withClassLoader(classLoader, delegate::finish);
    }

    @Override
    public void abort()
    {
        withClassLoader(classLoader, delegate::abort);
    }
}
