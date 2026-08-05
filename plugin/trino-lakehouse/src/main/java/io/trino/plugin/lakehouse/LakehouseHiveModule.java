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
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveExecutorModule;
import io.trino.plugin.hive.HiveFormatsModule;
import io.trino.plugin.hive.HiveLocationService;
import io.trino.plugin.hive.HiveMetadataFactory;
import io.trino.plugin.hive.HiveNodePartitioningProvider;
import io.trino.plugin.hive.HivePageSinkProvider;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.HiveSessionProperties;
import io.trino.plugin.hive.HiveSplitManager;
import io.trino.plugin.hive.HiveTableProperties;
import io.trino.plugin.hive.HiveTransactionManager;
import io.trino.plugin.hive.HiveWriterStats;
import io.trino.plugin.hive.LocationService;
import io.trino.plugin.hive.PartitionUpdate;
import io.trino.plugin.hive.PartitionsSystemTableProvider;
import io.trino.plugin.hive.PropertiesSystemTableProvider;
import io.trino.plugin.hive.SystemTableProvider;
import io.trino.plugin.hive.TransactionalMetadataFactory;
import io.trino.plugin.hive.fs.CachingDirectoryLister;
import io.trino.plugin.hive.fs.DirectoryLister;
import io.trino.plugin.hive.fs.TransactionScopeCachingDirectoryListerFactory;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.HiveMetastoreModule;

import java.util.Optional;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

class LakehouseHiveModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new HiveMetastoreModule(Optional.empty(), false));

        configBinder(binder).bindConfig(HiveConfig.class);
        configBinder(binder).bindConfig(HiveMetastoreConfig.class);

        binder.bind(HiveNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(HivePageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(HivePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(HiveSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(HiveSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(HiveTableProperties.class).in(Scopes.SINGLETON);

        binder.bind(HiveTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(HiveMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(TransactionalMetadataFactory.class).to(HiveMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(HivePartitionManager.class).in(Scopes.SINGLETON);
        binder.bind(LocationService.class).to(HiveLocationService.class).in(Scopes.SINGLETON);
        binder.bind(TransactionScopeCachingDirectoryListerFactory.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(PartitionUpdate.class);

        binder.bind(HiveWriterStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(HiveWriterStats.class).withGeneratedName();

        binder.bind(CachingDirectoryLister.class).in(Scopes.SINGLETON);
        newExporter(binder).export(CachingDirectoryLister.class).withGeneratedName();
        binder.bind(DirectoryLister.class).to(CachingDirectoryLister.class).in(Scopes.SINGLETON);

        var systemTableProviders = newSetBinder(binder, SystemTableProvider.class);
        systemTableProviders.addBinding().to(PartitionsSystemTableProvider.class).in(Scopes.SINGLETON);
        systemTableProviders.addBinding().to(PropertiesSystemTableProvider.class).in(Scopes.SINGLETON);

        install(new HiveFormatsModule());
        binder.install(new HiveExecutorModule());
    }
}
