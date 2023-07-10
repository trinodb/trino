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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;
import io.trino.plugin.hive.metastore.DecoratedHiveMetastoreModule;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreFactory;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;

import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class TestingIcebergHiveMetastoreCatalogModule
        extends AbstractConfigurationAwareModule
{
    private final HiveMetastore hiveMetastore;
    private final ThriftMetastoreFactory thriftMetastoreFactory;

    public TestingIcebergHiveMetastoreCatalogModule(HiveMetastore hiveMetastore, ThriftMetastoreFactory thriftMetastoreFactory)
    {
        this.hiveMetastore = requireNonNull(hiveMetastore, "hiveMetastore is null");
        this.thriftMetastoreFactory = requireNonNull(thriftMetastoreFactory, "thriftMetastoreFactory is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        install(new DecoratedHiveMetastoreModule(false));
        binder.bind(ThriftMetastoreFactory.class).toInstance(this.thriftMetastoreFactory);
        binder.bind(HiveMetastoreFactory.class).annotatedWith(RawHiveMetastoreFactory.class).toInstance(HiveMetastoreFactory.ofInstance(this.hiveMetastore));
        binder.bind(IcebergTableOperationsProvider.class).to(HiveMetastoreTableOperationsProvider.class).in(Scopes.SINGLETON);
        binder.bind(TrinoCatalogFactory.class).to(TrinoHiveCatalogFactory.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfigDefaults(CachingHiveMetastoreConfig.class, config -> {
            // ensure caching metastore wrapper isn't created, as it's not leveraged by Iceberg
            config.setStatsCacheTtl(new Duration(0, TimeUnit.SECONDS));
        });
    }
}
