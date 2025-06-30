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
package io.trino.plugin.deltalake.metastore.polaris;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.deltalake.AllowDeltaLakeManagedTableRename;
import io.trino.plugin.deltalake.MaxTableParameterLength;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.plugin.deltalake.metastore.file.DeltaLakeFileMetastoreTableOperationsProvider;
import io.trino.plugin.deltalake.transactionlog.writer.LocalTransactionLogSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogSynchronizer;

import static com.google.inject.multibindings.MapBinder.newMapBinder;

public class DeltaLakePolarisMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(DeltaLakeTableOperationsProvider.class).to(DeltaLakeFileMetastoreTableOperationsProvider.class).in(Scopes.SINGLETON);
        binder.bind(Key.get(boolean.class, AllowDeltaLakeManagedTableRename.class)).toInstance(false);
        binder.bind(Key.get(int.class, MaxTableParameterLength.class)).toInstance(512000);

        MapBinder<String, TransactionLogSynchronizer> synchronizerBinder = newMapBinder(binder, String.class, TransactionLogSynchronizer.class);
        synchronizerBinder.addBinding("file").to(LocalTransactionLogSynchronizer.class).in(Scopes.SINGLETON);
    }
}
