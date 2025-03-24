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
package io.trino.plugin.hive;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.trino.spi.catalog.CatalogName;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.concurrent.Threads.virtualThreadsNamed;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newThreadPerTaskExecutor;

public class HiveExecutorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForHiveMetadata.class));
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForHiveSplitManager.class));
        closingBinder(binder).registerExecutor(Key.get(ScheduledExecutorService.class, ForHiveTransactionHeartbeats.class));
    }

    @Provides
    @Singleton
    @ForHiveMetadata
    public ExecutorService createMetadataExecutor(CatalogName catalogName)
    {
        return newThreadPerTaskExecutor(virtualThreadsNamed("hive-metadata-" + catalogName + "-%d"));
    }

    @Provides
    @Singleton
    @ForHiveSplitManager
    public ExecutorService createSplitSourceExecutor(CatalogName catalogName)
    {
        return newThreadPerTaskExecutor(virtualThreadsNamed("hive-split-source-" + catalogName + "-%d"));
    }

    @Provides
    @Singleton
    @ForHiveTransactionHeartbeats
    public ScheduledExecutorService createHiveTransactionHeartbeatExecutor(CatalogName catalogName, HiveConfig hiveConfig)
    {
        return newScheduledThreadPool(
                hiveConfig.getHiveTransactionHeartbeatThreads(),
                daemonThreadsNamed("hive-heartbeat-" + catalogName + "-%s"));
    }
}
