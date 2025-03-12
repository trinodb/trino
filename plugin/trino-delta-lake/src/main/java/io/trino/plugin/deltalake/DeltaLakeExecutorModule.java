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
package io.trino.plugin.deltalake;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.trino.spi.catalog.CatalogName;

import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class DeltaLakeExecutorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForDeltaLakeMetadata.class));
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForDeltaLakeSplitManager.class));
    }

    @Provides
    @Singleton
    @ForDeltaLakeMetadata
    public ExecutorService createMetadataExecutor(CatalogName catalogName)
    {
        return newCachedThreadPool(daemonThreadsNamed("delta-metadata-" + catalogName + "-%s"));
    }

    @Provides
    @Singleton
    @ForDeltaLakeSplitManager
    public ExecutorService createSplitSourceExecutor(CatalogName catalogName)
    {
        return newCachedThreadPool(daemonThreadsNamed("delta-split-source-" + catalogName + "-%s"));
    }
}
