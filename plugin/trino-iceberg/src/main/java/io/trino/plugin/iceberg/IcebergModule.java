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
package io.trino.plugin.iceberg;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.trino.plugin.base.security.AllowAllAccessControl;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveNodePartitioningProvider;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.procedure.Procedure;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(IcebergTransactionManager.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(HiveConfig.class);
        configBinder(binder).bindConfig(IcebergConfig.class);
        configBinder(binder).bindConfig(MetastoreConfig.class);

        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(IcebergSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(IcebergTableProperties.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).to(IcebergSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorPageSourceProvider.class).setDefault().to(IcebergPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(IcebergPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(OrcReaderConfig.class);
        configBinder(binder).bindConfig(OrcWriterConfig.class);

        configBinder(binder).bindConfig(ParquetReaderConfig.class);
        configBinder(binder).bindConfig(ParquetWriterConfig.class);

        binder.bind(TrinoCatalogFactory.class).in(Scopes.SINGLETON);
        binder.bind(IcebergMetadataFactory.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(CommitTaskData.class);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        binder.bind(HiveTableOperationsProvider.class).in(Scopes.SINGLETON);

        binder.bind(IcebergFileWriterFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IcebergFileWriterFactory.class).withGeneratedName();

        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(RollbackToSnapshotProcedure.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, ConnectorAccessControl.class).setDefault().to(AllowAllAccessControl.class).in(Scopes.SINGLETON);
    }
}
