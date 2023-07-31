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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.glue.model.Table;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hive.HideDeltaLakeTables;
import io.trino.plugin.hive.metastore.glue.ForGlueHiveMetastore;
import io.trino.plugin.hive.metastore.glue.GlueCredentialsProvider;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreModule;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.procedure.MigrateProcedure;
import io.trino.spi.procedure.Procedure;

import java.util.function.Predicate;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergGlueCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(GlueHiveMetastoreConfig.class);
        configBinder(binder).bindConfig(IcebergGlueCatalogConfig.class);
        binder.bind(GlueMetastoreStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(GlueMetastoreStats.class).withGeneratedName();
        binder.bind(AWSCredentialsProvider.class).toProvider(GlueCredentialsProvider.class).in(Scopes.SINGLETON);
        binder.bind(IcebergTableOperationsProvider.class).to(GlueIcebergTableOperationsProvider.class).in(Scopes.SINGLETON);
        binder.bind(TrinoCatalogFactory.class).to(TrinoGlueCatalogFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TrinoCatalogFactory.class).withGeneratedName();

        install(conditionalModule(
                IcebergGlueCatalogConfig.class,
                IcebergGlueCatalogConfig::isSkipArchive,
                internalBinder -> newSetBinder(internalBinder, RequestHandler2.class, ForGlueHiveMetastore.class).addBinding().toInstance(new SkipArchiveRequestHandler())));

        // Required to inject HiveMetastoreFactory for migrate procedure
        binder.bind(Key.get(boolean.class, HideDeltaLakeTables.class)).toInstance(false);
        newOptionalBinder(binder, Key.get(new TypeLiteral<Predicate<Table>>() {}, ForGlueHiveMetastore.class))
                .setBinding().toInstance(table -> true);
        install(new GlueMetastoreModule());
        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(MigrateProcedure.class).in(Scopes.SINGLETON);
    }
}
