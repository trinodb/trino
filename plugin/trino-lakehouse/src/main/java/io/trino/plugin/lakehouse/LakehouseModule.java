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
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.deltalake.procedure.DropExtendedStatsProcedure;
import io.trino.plugin.deltalake.procedure.FlushMetadataCacheProcedure;
import io.trino.plugin.deltalake.procedure.RegisterTableProcedure;
import io.trino.plugin.deltalake.procedure.UnregisterTableProcedure;
import io.trino.plugin.deltalake.procedure.VacuumProcedure;
import io.trino.plugin.hive.HideDeltaLakeTables;
import io.trino.plugin.hive.SortingFileWriterConfig;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.hive.procedure.CreateEmptyPartitionProcedure;
import io.trino.plugin.hive.procedure.DropStatsProcedure;
import io.trino.plugin.hive.procedure.RegisterPartitionProcedure;
import io.trino.plugin.hive.procedure.SyncPartitionMetadataProcedure;
import io.trino.plugin.hive.procedure.UnregisterPartitionProcedure;
import io.trino.plugin.iceberg.procedure.RollbackToSnapshotProcedure;
import io.trino.plugin.lakehouse.procedures.LakehouseDropStatsProcedure;
import io.trino.plugin.lakehouse.procedures.LakehouseFlushMetadataCacheProcedure;
import io.trino.plugin.lakehouse.procedures.LakehouseRegisterTableProcedure;
import io.trino.plugin.lakehouse.procedures.LakehouseUnregisterTableProcedure;
import io.trino.spi.procedure.Procedure;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

class LakehouseModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(LakehouseConfig.class);
        configBinder(binder).bindConfig(OrcReaderConfig.class);
        configBinder(binder).bindConfig(OrcWriterConfig.class);
        configBinder(binder).bindConfig(ParquetReaderConfig.class);
        configBinder(binder).bindConfig(ParquetWriterConfig.class);
        configBinder(binder).bindConfig(SortingFileWriterConfig.class, "lakehouse");

        binder.bind(LakehouseConnector.class).in(Scopes.SINGLETON);
        binder.bind(LakehouseNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(LakehousePageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(LakehousePageSourceProviderFactory.class).in(Scopes.SINGLETON);
        binder.bind(LakehouseSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(LakehouseSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(LakehouseTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(LakehouseTransactionManager.class).in(Scopes.SINGLETON);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        // DeltaLake procedures
        procedures.addBinding().toProvider(VacuumProcedure.class).in(Scopes.SINGLETON);
        // Hive procedures
        procedures.addBinding().toProvider(CreateEmptyPartitionProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(RegisterPartitionProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(UnregisterPartitionProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(SyncPartitionMetadataProcedure.class).in(Scopes.SINGLETON);
        // Iceberg procedures
        procedures.addBinding().toProvider(RollbackToSnapshotProcedure.class).in(Scopes.SINGLETON);
        // Mixed procedures
        binder.bind(DropExtendedStatsProcedure.class).in(Scopes.SINGLETON);
        binder.bind(DropStatsProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(LakehouseDropStatsProcedure.class).in(Scopes.SINGLETON);

        binder.bind(RegisterTableProcedure.class).in(Scopes.SINGLETON);
        binder.bind(io.trino.plugin.iceberg.procedure.RegisterTableProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(LakehouseRegisterTableProcedure.class).in(Scopes.SINGLETON);

        binder.bind(UnregisterTableProcedure.class).in(Scopes.SINGLETON);
        binder.bind(io.trino.plugin.iceberg.procedure.UnregisterTableProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(LakehouseUnregisterTableProcedure.class).in(Scopes.SINGLETON);

        binder.bind(FlushMetadataCacheProcedure.class).in(Scopes.SINGLETON);
        binder.bind(io.trino.plugin.hive.procedure.FlushMetadataCacheProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(LakehouseFlushMetadataCacheProcedure.class).in(Scopes.SINGLETON);

        binder.bind(Key.get(boolean.class, HideDeltaLakeTables.class)).toInstance(false);
    }
}
