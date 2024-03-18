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
package io.trino.plugin.warp.di;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.engine.StubExceptionThrower;
import io.trino.plugin.varada.storage.engine.StubsConnectorSync;
import io.trino.plugin.varada.storage.engine.StubsStorageEngine;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.varada.storage.read.RangeFillerService;
import io.trino.plugin.varada.storage.read.StubsRangeFillerService;

/**
 * Module for binding
 * Should only be used for Testing, never be used in Production.
 */
public class VaradaStubsStorageEngineModule
        implements Module
{
    private StorageEngineConstants storageEngineConstants = new StubsStorageEngineConstants(100);
    private StorageEngine storageEngine = new StubsStorageEngine();
    private final RangeFillerService rangeFillerService = new StubsRangeFillerService();

    @Override
    public void configure(Binder binder)
    {
        binder.bind(StorageEngine.class).toInstance(storageEngine);
        binder.bind(RangeFillerService.class).toInstance(rangeFillerService);
        binder.bind(StorageEngineConstants.class).toInstance(storageEngineConstants);
        binder.bind(ConnectorSync.class).to(StubsConnectorSync.class);

        NativeStorageStateHandler nativeStorageStateHandler = new NativeStorageStateHandler(
                new NativeConfiguration(),
                new StubExceptionThrower(),
                new GlobalConfiguration());
        binder.bind(NativeStorageStateHandler.class).toInstance(nativeStorageStateHandler);
    }

    public VaradaStubsStorageEngineModule withStorageEngine(StorageEngine storageEngine)
    {
        this.storageEngine = storageEngine;
        return this;
    }

    public VaradaStubsStorageEngineModule withStorageEngineConstants(StorageEngineConstants storageEngineConstants)
    {
        this.storageEngineConstants = storageEngineConstants;
        return this;
    }

    public StorageEngine getStorageEngine()
    {
        return storageEngine;
    }

    public StorageEngineConstants getStorageEngineConstants()
    {
        return storageEngineConstants;
    }
}
