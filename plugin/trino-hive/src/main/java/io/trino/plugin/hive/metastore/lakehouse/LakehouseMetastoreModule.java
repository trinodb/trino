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
package io.trino.plugin.hive.metastore.lakehouse;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.trace.Tracer;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.metastore.lakehouse.LakehouseHiveMetastoreFactory;
import io.trino.metastore.lakehouse.LakehouseMetastoreConfig;
import io.trino.plugin.hive.AllowHiveTableRename;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.bootstrap.ClosingBinder.closingBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public final class LakehouseMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(LakehouseMetastoreConfig.class);

        newOptionalBinder(binder, Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class))
                .setDefault()
                .to(LakehouseHiveMetastoreFactory.class)
                .in(Scopes.SINGLETON);
        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(false);

        closingBinder(binder).registerCloseable(LakehouseHiveMetastoreFactory.class);
    }

    @Provides
    @Singleton
    public LakehouseHiveMetastoreFactory createLakehouseHiveMetastoreFactory(LakehouseMetastoreConfig config, Tracer tracer)
    {
        return new LakehouseHiveMetastoreFactory(config, tracer);
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof LakehouseMetastoreModule;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
