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
package io.trino.plugin.hudi;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class HudiExecutorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForHudiSplitManager.class));
        closingBinder(binder).registerExecutor(Key.get(ScheduledExecutorService.class, ForHudiSplitSource.class));
    }

    @Provides
    @Singleton
    @ForHudiSplitManager
    public ExecutorService createExecutorService()
    {
        return newCachedThreadPool(daemonThreadsNamed("hudi-split-manager-%s"));
    }

    @Provides
    @Singleton
    @ForHudiSplitSource
    public ScheduledExecutorService createSplitLoaderExecutor(HudiConfig hudiConfig)
    {
        return newScheduledThreadPool(
                hudiConfig.getSplitLoaderParallelism(),
                daemonThreadsNamed("hudi-split-loader-%s"));
    }
}
