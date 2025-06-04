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
package io.trino.plugin.deltalake.metastore.thrift;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.deltalake.AllowDeltaLakeManagedTableRename;
import io.trino.plugin.deltalake.MaxTableParameterLength;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreModule;

public class DeltaLakeThriftMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new ThriftMetastoreModule());
        binder.bind(DeltaLakeTableOperationsProvider.class).to(DeltaLakeThriftMetastoreTableOperationsProvider.class).in(Scopes.SINGLETON);
        binder.bind(Key.get(boolean.class, AllowDeltaLakeManagedTableRename.class)).toInstance(false);
        // Limit per Hive metastore code (https://github.com/apache/hive/tree/7f6367e0c6e21b11ef62da1ea6681a54d547de07/standalone-metastore/metastore-server/src/main/sql as of this writing)
        // - MySQL: mediumtext (16777215)
        // - SQL Server: nvarchar(max) (2147483647)
        // - Oracle: clob (4294967295)
        // - PostgreSQL: text (unlimited)
        binder.bind(Key.get(int.class, MaxTableParameterLength.class)).toInstance(16777215);
    }
}
