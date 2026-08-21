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
package io.trino.plugin.lakehouse;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hudi.ForHudiSplitManager;
import io.trino.plugin.hudi.ForHudiSplitSource;
import io.trino.plugin.hudi.HudiConfig;
import io.trino.plugin.hudi.HudiMetadataFactory;
import io.trino.plugin.hudi.HudiPageSourceProvider;
import io.trino.plugin.hudi.HudiSessionProperties;
import io.trino.plugin.hudi.HudiSplitManager;
import io.trino.plugin.hudi.HudiTableProperties;
import io.trino.plugin.hudi.HudiTransactionManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.bootstrap.ClosingBinder.closingBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class LakehouseHudiModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(HudiConfig.class);

        binder.bind(HudiPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(HudiSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(HudiSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(HudiTableProperties.class).in(Scopes.SINGLETON);

        binder.bind(HudiTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(HudiMetadataFactory.class).in(Scopes.SINGLETON);

        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForHudiSplitManager.class));
        closingBinder(binder).registerExecutor(Key.get(ScheduledExecutorService.class, ForHudiSplitSource.class));
    }

    @Provides
    @Singleton
    @ForHudiSplitManager
    public ExecutorService createSplitManagerExecutor()
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
