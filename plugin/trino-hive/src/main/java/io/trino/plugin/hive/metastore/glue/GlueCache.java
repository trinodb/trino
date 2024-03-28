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

public interface GlueCache
{
    GlueCache NOOP = new NoopGlueCache();

    List<String> getDatabaseNames(Function<Consumer<Database>, List<String>> loader);

    /**
     * Invalidate the database cache and cascade to all nested elements in the database (table, partition, function, etc.).
     */
    void invalidateDatabase(String databaseName);

    void invalidateDatabaseNames();

    Optional<Database> getDatabase(String databaseName, Supplier<Optional<Database>> loader);

    List<TableInfo> getTables(String databaseName, Function<Consumer<Table>, List<TableInfo>> loader);

    void invalidateTables(String databaseName);

    Optional<Table> getTable(String databaseName, String tableName, Supplier<Optional<Table>> loader);

    void invalidateTable(String databaseName, String tableName, boolean cascade);

    Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames, Function<Set<String>, Map<String, HiveColumnStatistics>> loader);

    void invalidateTableColumnStatistics(String databaseName, String tableName);

    Set<PartitionName> getPartitionNames(String databaseName, String tableName, String glueExpression, Function<Consumer<Partition>, Set<PartitionName>> loader);

    Optional<Partition> getPartition(String databaseName, String tableName, PartitionName partitionName, Supplier<Optional<Partition>> loader);

    Collection<Partition> batchGetPartitions(
            String databaseName,
            String tableName,
            Collection<PartitionName> partitionNames,
            BiFunction<Consumer<Partition>, Collection<PartitionName>, Collection<Partition>> loader);

    void invalidatePartition(String databaseName, String tableName, PartitionName partitionName);

    Map<String, HiveColumnStatistics> getPartitionColumnStatistics(
            String databaseName,
            String tableName,
            PartitionName partitionName,
            Set<String> columnNames,
            Function<Set<String>, Map<String, HiveColumnStatistics>> loader);

    Collection<LanguageFunction> getAllFunctions(String databaseName, Supplier<Collection<LanguageFunction>> loader);

    Collection<LanguageFunction> getFunction(String databaseName, String functionName, Supplier<Collection<LanguageFunction>> loader);

    void invalidateFunction(String databaseName, String functionName);
}
