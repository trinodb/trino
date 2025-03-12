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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.trino.spi.catalog.CatalogName;

import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class IcebergExecutorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForIcebergMetadata.class));
        closingBinder(binder).registerExecutor(Key.get(ListeningExecutorService.class, ForIcebergSplitManager.class));
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForIcebergScanPlanning.class));
    }

    @Singleton
    @Provides
    @ForIcebergMetadata
    public ExecutorService createIcebergMetadataExecutor(CatalogName catalogName)
    {
        return newCachedThreadPool(daemonThreadsNamed("iceberg-metadata-" + catalogName + "-%s"));
    }

    @Provides
    @Singleton
    @ForIcebergSplitManager
    public ListeningExecutorService createSplitSourceExecutor(CatalogName catalogName)
    {
        return listeningDecorator(newCachedThreadPool(daemonThreadsNamed("iceberg-split-source-" + catalogName + "-%s")));
    }

    @Provides
    @Singleton
    @ForIcebergScanPlanning
    public ExecutorService createScanPlanningExecutor(CatalogName catalogName, IcebergConfig config)
    {
        if (config.getSplitManagerThreads() == 0) {
            return newDirectExecutorService();
        }
        return newFixedThreadPool(
                config.getSplitManagerThreads(),
                daemonThreadsNamed("iceberg-split-manager-" + catalogName + "-%s"));
    }
}
