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

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.plugin.deltalake.transactionlog.writer.AzureTransactionLogSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.GcsTransactionLogSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.S3ConditionalWriteLogSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.S3LockBasedTransactionLogSynchronizer;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogSynchronizer;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class DeltaLakeSynchronizerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        var synchronizerBinder = newMapBinder(binder, String.class, TransactionLogSynchronizer.class);

        // Azure
        synchronizerBinder.addBinding("abfs").to(AzureTransactionLogSynchronizer.class).in(Scopes.SINGLETON);
        synchronizerBinder.addBinding("abfss").to(AzureTransactionLogSynchronizer.class).in(Scopes.SINGLETON);

        // GCS
        synchronizerBinder.addBinding("gs").to(GcsTransactionLogSynchronizer.class).in(Scopes.SINGLETON);

        // S3
        jsonCodecBinder(binder).bindJsonCodec(S3LockBasedTransactionLogSynchronizer.LockFileContents.class);
        binder.bind(S3LockBasedTransactionLogSynchronizer.class).in(Scopes.SINGLETON);
        binder.bind(S3ConditionalWriteLogSynchronizer.class).in(Scopes.SINGLETON);

        install(conditionalModule(S3FileSystemConfig.class, S3FileSystemConfig::isSupportsExclusiveCreate,
                s3SynchronizerModule(S3ConditionalWriteLogSynchronizer.class),
                s3SynchronizerModule(S3LockBasedTransactionLogSynchronizer.class)));
    }

    private static Module s3SynchronizerModule(Class<? extends TransactionLogSynchronizer> synchronizerClass)
    {
        return new AbstractModule()
        {
            @Override
            protected void configure()
            {
                var synchronizerBinder = newMapBinder(binder(), String.class, TransactionLogSynchronizer.class);
                synchronizerBinder.addBinding("s3").to(synchronizerClass).in(Scopes.SINGLETON);
                synchronizerBinder.addBinding("s3a").to(synchronizerClass).in(Scopes.SINGLETON);
                synchronizerBinder.addBinding("s3n").to(synchronizerClass).in(Scopes.SINGLETON);
            }
        };
    }
}
