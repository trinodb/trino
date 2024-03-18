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
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.ExceptionThrower;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeStorageEngine;
import io.trino.plugin.varada.util.FailureGeneratorInvocationHandler;

import java.lang.reflect.Proxy;

import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerStorageEngineProvider
        implements Provider<StorageEngine>
{
    private final GlobalConfiguration globalConfiguration;
    private final NativeConfiguration nativeConfiguration;
    private final MetricsManager metricsManager;
    private final ExceptionThrower exceptionThrower;
    private final FailureGeneratorInvocationHandler failureGeneratorInvocationHandler;
    private StorageEngine storageEngine;

    @Inject
    public WorkerStorageEngineProvider(
            GlobalConfiguration globalConfiguration,
            NativeConfiguration nativeConfiguration,
            MetricsManager metricsManager,
            ExceptionThrower exceptionThrower,
            FailureGeneratorInvocationHandler failureGeneratorInvocationHandler)
    {
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.nativeConfiguration = requireNonNull(nativeConfiguration);
        this.metricsManager = requireNonNull(metricsManager);
        this.exceptionThrower = requireNonNull(exceptionThrower);
        this.failureGeneratorInvocationHandler = requireNonNull(failureGeneratorInvocationHandler);
    }

    @Override
    public synchronized StorageEngine get()
    {
        if (storageEngine == null) {
            storageEngine = new NativeStorageEngine(
                    nativeConfiguration,
                    metricsManager,
                    exceptionThrower);

            if (globalConfiguration.isFailureGeneratorEnabled()) {
                storageEngine = (StorageEngine) Proxy.newProxyInstance(storageEngine.getClass().getClassLoader(),
                        new Class<?>[] {StorageEngine.class},
                        failureGeneratorInvocationHandler.getMethodInvocationHandler(storageEngine));
            }
        }
        return storageEngine;
    }
}
