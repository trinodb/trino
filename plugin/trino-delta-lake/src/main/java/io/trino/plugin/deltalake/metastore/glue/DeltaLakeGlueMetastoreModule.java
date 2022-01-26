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
package io.trino.plugin.deltalake.metastore.glue;

import com.amazonaws.services.glue.model.Table;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.deltalake.HideNonDeltaLakeTables;
import io.trino.plugin.hive.metastore.glue.ForGlueHiveMetastore;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreModule;

import java.util.function.Predicate;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class DeltaLakeGlueMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(DeltaLakeGlueMetastoreConfig.class);

        newOptionalBinder(binder, Key.get(new TypeLiteral<Predicate<Table>>() {}, ForGlueHiveMetastore.class))
                .setBinding().toProvider(DeltaLakeGlueMetastoreTableFilterProvider.class);

        install(new GlueMetastoreModule());
    }

    @Provides
    @Singleton
    @HideNonDeltaLakeTables
    public boolean provideHideNonDeltaLakeTables(DeltaLakeGlueMetastoreConfig glueMetastoreConfig)
    {
        return glueMetastoreConfig.isHideNonDeltaLakeTables();
    }
}
