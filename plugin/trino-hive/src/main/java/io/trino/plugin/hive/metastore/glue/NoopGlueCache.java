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
package io.trino.plugin.hive.metastore.glue;

import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.TableInfo;
import io.trino.spi.function.LanguageFunction;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

class NoopGlueCache
        implements GlueCache
{
    @Override
    public List<String> getDatabaseNames(Function<Consumer<Database>, List<String>> loader)
    {
        return loader.apply(database -> {});
    }

    @Override
    public void invalidateDatabase(String databaseName) {}

    @Override
    public void invalidateDatabaseNames() {}

    @Override
    public Optional<Database> getDatabase(String databaseName, Supplier<Optional<Database>> loader)
    {
        return loader.get();
    }

    @Override
    public List<TableInfo> getTables(String databaseName, Function<Consumer<Table>, List<TableInfo>> loader)
    {
        return loader.apply(table -> {});
    }

    @Override
    public void invalidateTables(String databaseName) {}

    @Override
    public Optional<Table> getTable(String databaseName, String tableName, Supplier<Optional<Table>> loader)
    {
        return loader.get();
    }

    @Override
    public void invalidateTable(String databaseName, String tableName, boolean cascade) {}

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames, Function<Set<String>, Map<String, HiveColumnStatistics>> loader)
    {
        return loader.apply(columnNames);
    }

    @Override
    public void invalidateTableColumnStatistics(String databaseName, String tableName) {}

    @Override
    public Set<PartitionName> getPartitionNames(String databaseName, String tableName, String glueExpression, Function<Consumer<Partition>, Set<PartitionName>> loader)
    {
        return loader.apply(partition -> {});
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, PartitionName partitionName, Supplier<Optional<Partition>> loader)
    {
        return loader.get();
    }

    @Override
    public Collection<Partition> batchGetPartitions(String databaseName, String tableName, Collection<PartitionName> partitionNames, BiFunction<Consumer<Partition>, Collection<PartitionName>, Collection<Partition>> loader)
    {
        return loader.apply(partition -> {}, partitionNames);
    }

    @Override
    public void invalidatePartition(String databaseName, String tableName, PartitionName partitionName) {}

    @Override
    public Map<String, HiveColumnStatistics> getPartitionColumnStatistics(String databaseName, String tableName, PartitionName partitionName, Set<String> columnNames, Function<Set<String>, Map<String, HiveColumnStatistics>> loader)
    {
        return loader.apply(columnNames);
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName, Supplier<Collection<LanguageFunction>> loader)
    {
        return loader.get();
    }

    @Override
    public Collection<LanguageFunction> getFunction(String databaseName, String functionName, Supplier<Collection<LanguageFunction>> loader)
    {
        return loader.get();
    }

    @Override
    public void invalidateFunction(String databaseName, String functionName) {}
}
