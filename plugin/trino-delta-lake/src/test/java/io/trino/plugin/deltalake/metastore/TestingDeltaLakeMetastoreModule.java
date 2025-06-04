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
package io.trino.plugin.deltalake.metastore;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.deltalake.AllowDeltaLakeManagedTableRename;
import io.trino.plugin.deltalake.MaxTableParameterLength;
import io.trino.plugin.deltalake.metastore.file.DeltaLakeFileMetastoreTableOperationsProvider;
import io.trino.plugin.hive.HideDeltaLakeTables;
import io.trino.plugin.hive.metastore.CachingHiveMetastoreModule;

import static java.util.Objects.requireNonNull;

public class TestingDeltaLakeMetastoreModule
        extends AbstractConfigurationAwareModule
{
    private final HiveMetastore metastore;

    public TestingDeltaLakeMetastoreModule(HiveMetastore metastore)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public void setup(Binder binder)
    {
        binder.bind(HiveMetastoreFactory.class).annotatedWith(RawHiveMetastoreFactory.class).toInstance(HiveMetastoreFactory.ofInstance(metastore));
        install(new CachingHiveMetastoreModule());
        binder.bind(DeltaLakeTableOperationsProvider.class).to(DeltaLakeFileMetastoreTableOperationsProvider.class).in(Scopes.SINGLETON);

        binder.bind(Key.get(boolean.class, HideDeltaLakeTables.class)).toInstance(false);
        binder.bind(Key.get(boolean.class, AllowDeltaLakeManagedTableRename.class)).toInstance(true);
        binder.bind(Key.get(int.class, MaxTableParameterLength.class)).toInstance(Integer.MAX_VALUE);
    }
}
