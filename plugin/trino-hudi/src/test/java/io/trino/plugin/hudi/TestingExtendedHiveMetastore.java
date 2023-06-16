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
package io.trino.plugin.hudi;

import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.UnimplementedHiveMetastore;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TestingExtendedHiveMetastore
        extends UnimplementedHiveMetastore
{
    private final Table table;
    private final List<String> partitions;

    public TestingExtendedHiveMetastore(Table table, List<String> partitions)
    {
        this.table = requireNonNull(table, "table is null");
        this.partitions = requireNonNull(partitions, "partitions is null");
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return Optional.of(table);
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return Optional.of(partitions);
    }
}
