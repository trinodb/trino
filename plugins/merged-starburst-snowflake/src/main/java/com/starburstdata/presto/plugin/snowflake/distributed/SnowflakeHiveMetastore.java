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
package com.starburstdata.presto.plugin.snowflake.distributed;

import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Table;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

class SnowflakeHiveMetastore
        extends UnimplementedHiveMetastore
{
    private final Table table;

    SnowflakeHiveMetastore(Table table)
    {
        this.table = requireNonNull(table, "table is null");
    }

    public Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName)
    {
        checkArgument(table.getDatabaseName().toLowerCase(ENGLISH).equals(databaseName), "databaseName does not match");
        checkArgument(table.getTableName().toLowerCase(ENGLISH).equals(tableName), "tableName does not match");

        return Optional.of(table);
    }
}
