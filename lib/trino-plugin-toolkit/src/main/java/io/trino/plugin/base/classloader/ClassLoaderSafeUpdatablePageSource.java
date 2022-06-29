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

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeUpdatablePageSource
        extends ClassLoaderSafeConnectorPageSource
        implements UpdatablePageSource
{
    protected final UpdatablePageSource updatablePageSource;

    @Inject
    public ClassLoaderSafeUpdatablePageSource(@ForClassLoaderSafe UpdatablePageSource delegate, ClassLoader classLoader)
    {
        super(delegate, classLoader);
        this.updatablePageSource = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            updatablePageSource.deleteRows(rowIds);
        }
    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            updatablePageSource.updateRows(page, columnValueAndRowIdChannels);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return updatablePageSource.finish();
        }
    }

    @Override
    public void abort()
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            updatablePageSource.abort();
        }
    }
}
