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
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.MetastoreTypeConfig;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergExecutorModule;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergFileWriterFactory;
import io.trino.plugin.iceberg.IcebergMaterializedViewProperties;
import io.trino.plugin.iceberg.IcebergMetadataFactory;
import io.trino.plugin.iceberg.IcebergNodePartitioningProvider;
import io.trino.plugin.iceberg.IcebergPageSinkProvider;
import io.trino.plugin.iceberg.IcebergPageSourceProviderFactory;
import io.trino.plugin.iceberg.IcebergSessionProperties;
import io.trino.plugin.iceberg.IcebergSplitManager;
import io.trino.plugin.iceberg.IcebergTableProperties;
import io.trino.plugin.iceberg.IcebergTransactionManager;
import io.trino.plugin.iceberg.TableStatisticsWriter;
import io.trino.plugin.iceberg.catalog.file.IcebergFileMetastoreCatalogModule;
import io.trino.plugin.iceberg.catalog.glue.IcebergGlueCatalogModule;
import io.trino.plugin.iceberg.catalog.hms.IcebergHiveMetastoreCatalogModule;
import io.trino.plugin.iceberg.catalog.rest.DefaultIcebergFileSystemFactory;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogModule;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class LakehouseIcebergModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(IcebergConfig.class);

        binder.bind(IcebergNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(IcebergPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(IcebergPageSourceProviderFactory.class).in(Scopes.SINGLETON);
        binder.bind(IcebergSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(IcebergSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(IcebergTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(IcebergMaterializedViewProperties.class).in(Scopes.SINGLETON);

        binder.bind(IcebergTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(IcebergMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(IcebergFileWriterFactory.class).in(Scopes.SINGLETON);
        binder.bind(TableStatisticsWriter.class).in(Scopes.SINGLETON);
        binder.bind(IcebergFileSystemFactory.class).to(DefaultIcebergFileSystemFactory.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class));

        jsonCodecBinder(binder).bindJsonCodec(CommitTaskData.class);

        install(switch (buildConfigObject(MetastoreTypeConfig.class).getMetastoreType()) {
            case THRIFT -> new IcebergHiveMetastoreCatalogModule();
            case FILE -> new IcebergFileMetastoreCatalogModule();
            case GLUE -> new IcebergGlueCatalogModule();
            case POLARIS -> new IcebergRestCatalogModule();
        });

        binder.install(new IcebergExecutorModule());
    }
}
