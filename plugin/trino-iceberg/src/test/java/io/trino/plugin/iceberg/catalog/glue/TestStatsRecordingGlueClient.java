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

import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.Partition;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.metastore.glue.GlueCache;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.hive.metastore.glue.PartitionName;
import io.trino.spi.function.LanguageFunction;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestStatsRecordingGlueClient
{
    @Test
    void testCacheInvalidationOnSuccess()
    {
        RecordingGlueCache glueCache = new RecordingGlueCache();
        StatsRecordingGlueClient client = new StatsRecordingGlueClient(new TestingGlueClient(false), glueCache, new GlueMetastoreStats());

        client.createDatabase(DatabaseInput.builder().name("db1").build());
        assertThat(glueCache.takeEvents()).containsExactly(
                "invalidateDatabase:db1",
                "invalidateDatabaseNames");

        client.dropDatabase("db1");
        assertThat(glueCache.takeEvents()).containsExactly(
                "invalidateDatabase:db1",
                "invalidateDatabaseNames");

        client.createTable("db1", TableInput.builder().name("tbl1").build());
        assertThat(glueCache.takeEvents()).containsExactly(
                "invalidateTable:db1.tbl1:cascade=true",
                "invalidateTables:db1");

        client.updateTable("db1", TableInput.builder().name("tbl1").build(), Optional.empty());
        assertThat(glueCache.takeEvents()).containsExactly(
                "invalidateTable:db1.tbl1:cascade=false");

        client.deleteTable("db1", "tbl1");
        assertThat(glueCache.takeEvents()).containsExactly(
                "invalidateTable:db1.tbl1:cascade=true",
                "invalidateTables:db1");
    }

    @Test
    void testCacheInvalidationOnFailure()
    {
        RecordingGlueCache glueCache = new RecordingGlueCache();
        StatsRecordingGlueClient client = new StatsRecordingGlueClient(new TestingGlueClient(true), glueCache, new GlueMetastoreStats());

        assertThatThrownBy(() -> client.createDatabase(DatabaseInput.builder().name("db1").build()))
                .isInstanceOf(GlueException.class);
        assertThat(glueCache.takeEvents()).containsExactly(
                "invalidateDatabase:db1",
                "invalidateDatabaseNames");

        assertThatThrownBy(() -> client.dropDatabase("db1"))
                .isInstanceOf(GlueException.class);
        assertThat(glueCache.takeEvents()).containsExactly(
                "invalidateDatabase:db1",
                "invalidateDatabaseNames");

        assertThatThrownBy(() -> client.createTable("db1", TableInput.builder().name("tbl1").build()))
                .isInstanceOf(GlueException.class);
        assertThat(glueCache.takeEvents()).containsExactly(
                "invalidateTable:db1.tbl1:cascade=true",
                "invalidateTables:db1");

        assertThatThrownBy(() -> client.updateTable("db1", TableInput.builder().name("tbl1").build(), Optional.empty()))
                .isInstanceOf(GlueException.class);
        assertThat(glueCache.takeEvents()).containsExactly(
                "invalidateTable:db1.tbl1:cascade=false");

        assertThatThrownBy(() -> client.deleteTable("db1", "tbl1"))
                .isInstanceOf(GlueException.class);
        assertThat(glueCache.takeEvents()).containsExactly(
                "invalidateTable:db1.tbl1:cascade=true",
                "invalidateTables:db1");
    }

    private static class TestingGlueClient
            implements GlueClient
    {
        private final boolean failOperations;

        private TestingGlueClient(boolean failOperations)
        {
            this.failOperations = failOperations;
        }

        private void checkFailure()
        {
            if (failOperations) {
                throw (GlueException) GlueException.builder().message("simulated failure").build();
            }
        }

        @Override
        public CreateDatabaseResponse createDatabase(CreateDatabaseRequest createDatabaseRequest)
        {
            checkFailure();
            return CreateDatabaseResponse.builder().build();
        }

        @Override
        public DeleteDatabaseResponse deleteDatabase(DeleteDatabaseRequest deleteDatabaseRequest)
        {
            checkFailure();
            return DeleteDatabaseResponse.builder().build();
        }

        @Override
        public CreateTableResponse createTable(CreateTableRequest createTableRequest)
        {
            checkFailure();
            return CreateTableResponse.builder().build();
        }

        @Override
        public UpdateTableResponse updateTable(UpdateTableRequest updateTableRequest)
        {
            checkFailure();
            return UpdateTableResponse.builder().build();
        }

        @Override
        public DeleteTableResponse deleteTable(DeleteTableRequest deleteTableRequest)
        {
            checkFailure();
            return DeleteTableResponse.builder().build();
        }

        @Override
        public String serviceName()
        {
            return "glue";
        }

        @Override
        public void close() {}
    }

    private static class RecordingGlueCache
            implements GlueCache
    {
        private final List<String> events = new ArrayList<>();

        public List<String> takeEvents()
        {
            List<String> copy = List.copyOf(events);
            events.clear();
            return copy;
        }

        @Override
        public void invalidateDatabase(String databaseName)
        {
            events.add("invalidateDatabase:" + databaseName);
        }

        @Override
        public void invalidateDatabaseNames()
        {
            events.add("invalidateDatabaseNames");
        }

        @Override
        public void invalidateTables(String databaseName)
        {
            events.add("invalidateTables:" + databaseName);
        }

        @Override
        public void invalidateTable(String databaseName, String tableName, boolean cascade)
        {
            events.add("invalidateTable:%s.%s:cascade=%s".formatted(databaseName, tableName, cascade));
        }

        @Override
        public List<String> getDatabaseNames(Function<Consumer<Database>, List<String>> loader)
        {
            return NOOP.getDatabaseNames(loader);
        }

        @Override
        public Optional<Database> getDatabase(String databaseName, Supplier<Optional<Database>> loader)
        {
            return NOOP.getDatabase(databaseName, loader);
        }

        @Override
        public List<TableInfo> getTables(String databaseName, Function<Consumer<Table>, List<TableInfo>> loader)
        {
            return NOOP.getTables(databaseName, loader);
        }

        @Override
        public Optional<Table> getTable(String databaseName, String tableName, Supplier<Optional<Table>> loader)
        {
            return NOOP.getTable(databaseName, tableName, loader);
        }

        @Override
        public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames, Function<Set<String>, Map<String, HiveColumnStatistics>> loader)
        {
            return NOOP.getTableColumnStatistics(databaseName, tableName, columnNames, loader);
        }

        @Override
        public void invalidateTableColumnStatistics(String databaseName, String tableName)
        {
            events.add("invalidateTableColumnStatistics:%s.%s".formatted(databaseName, tableName));
        }

        @Override
        public Set<PartitionName> getPartitionNames(String databaseName, String tableName, String glueExpression, Function<Consumer<Partition>, Set<PartitionName>> loader)
        {
            return NOOP.getPartitionNames(databaseName, tableName, glueExpression, loader);
        }

        @Override
        public Optional<Partition> getPartition(String databaseName, String tableName, PartitionName partitionName, Supplier<Optional<Partition>> loader)
        {
            return NOOP.getPartition(databaseName, tableName, partitionName, loader);
        }

        @Override
        public Collection<Partition> batchGetPartitions(String databaseName, String tableName, Collection<PartitionName> partitionNames, BiFunction<Consumer<Partition>, Collection<PartitionName>, Collection<Partition>> loader)
        {
            return NOOP.batchGetPartitions(databaseName, tableName, partitionNames, loader);
        }

        @Override
        public void invalidatePartition(String databaseName, String tableName, PartitionName partitionName)
        {
            events.add("invalidatePartition:%s.%s.%s".formatted(databaseName, tableName, partitionName));
        }

        @Override
        public Map<String, HiveColumnStatistics> getPartitionColumnStatistics(String databaseName, String tableName, PartitionName partitionName, Set<String> columnNames, Function<Set<String>, Map<String, HiveColumnStatistics>> loader)
        {
            return NOOP.getPartitionColumnStatistics(databaseName, tableName, partitionName, columnNames, loader);
        }

        @Override
        public Collection<LanguageFunction> getAllFunctions(String databaseName, Supplier<Collection<LanguageFunction>> loader)
        {
            return NOOP.getAllFunctions(databaseName, loader);
        }

        @Override
        public Collection<LanguageFunction> getFunction(String databaseName, String functionName, Supplier<Collection<LanguageFunction>> loader)
        {
            return NOOP.getFunction(databaseName, functionName, loader);
        }

        @Override
        public void invalidateFunction(String databaseName, String functionName)
        {
            events.add("invalidateFunction:%s.%s".formatted(databaseName, functionName));
        }

        @Override
        public void flushCache()
        {
            events.add("flushCache");
        }
    }
}
