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
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.deltalake.transactionlog.writer.GcsTransactionLogSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogSynchronizer;

import static com.google.inject.multibindings.MapBinder.newMapBinder;

public class DeltaLakeGcsModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(GcsStorageFactory.class).in(Scopes.SINGLETON);
        newMapBinder(binder, String.class, TransactionLogSynchronizer.class).addBinding("gs").to(GcsTransactionLogSynchronizer.class).in(Scopes.SINGLETON);
    }
}
