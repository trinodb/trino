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
package io.trino.plugin.varada.di;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.nativeimpl.DelegateStorageEngine;
import io.trino.plugin.varada.util.FailureGeneratorInvocationHandler;

import java.lang.reflect.Proxy;

@Singleton
public class CoordinatorStorageEngineProvider
        implements Provider<StorageEngine>
{
    private final GlobalConfiguration globalConfiguration;
    private final MetricsManager metricsManager;
    private final FailureGeneratorInvocationHandler failureGeneratorInvocationHandler;
    private StorageEngine storageEngine;

    @Inject
    public CoordinatorStorageEngineProvider(
            GlobalConfiguration globalConfiguration,
            MetricsManager metricsManager,
            FailureGeneratorInvocationHandler failureGeneratorInvocationHandler)
    {
        this.globalConfiguration = globalConfiguration;
        this.metricsManager = metricsManager;
        this.failureGeneratorInvocationHandler = failureGeneratorInvocationHandler;
    }

    @Override
    public synchronized StorageEngine get()
    {
        if (storageEngine == null) {
            this.storageEngine = new DelegateStorageEngine(metricsManager);
            if (globalConfiguration.isFailureGeneratorEnabled()) {
                this.storageEngine = (StorageEngine) Proxy.newProxyInstance(storageEngine.getClass().getClassLoader(),
                        new Class<?>[] {StorageEngine.class},
                        failureGeneratorInvocationHandler.getMethodInvocationHandler(storageEngine));
            }
        }
        return storageEngine;
    }
}
