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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.cache.NoopSplitAffinityProvider;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.io.Resources.getResource;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_PARTITION_NOT_FOUND;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that a failure in the background split loader is surfaced to the engine, rather than
 * being silently treated as a successful, empty split source.
 */
public class TestHudiSplitSource
{
    private static final String SCHEMA_NAME = "default";
    private static final String TABLE_NAME = "hudi_cow_pt_tbl";
    // Present on disk under src/test/resources/hudi-testing-data/hudi_cow_pt_tbl, but deliberately
    // never registered with the fake metastore below, so that loading it fails.
    private static final String MISSING_PARTITION_NAME = "dt=2021-12-09/hh=10";

    @Test
    public void testIsFinishedPropagatesSplitLoaderFailure()
            throws Exception
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new HudiSessionProperties(new HudiConfig(), new ParquetReaderConfig()).getSessionProperties())
                .build();

        String basePath = "local:" + new File(getResource("hudi-testing-data/" + TABLE_NAME).toURI()).getAbsolutePath();
        TrinoFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(Path.of("/"));

        List<Column> partitionColumns = List.of(
                new Column("dt", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("hh", HIVE_STRING, Optional.empty(), Map.of()));
        Table table = Table.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setTableName(TABLE_NAME)
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.of("public"))
                .setDataColumns(List.of(
                        new Column("id", HIVE_LONG, Optional.empty(), Map.of()),
                        new Column("name", HIVE_STRING, Optional.empty(), Map.of())))
                .setPartitionColumns(partitionColumns)
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(StorageFormat.create(PARQUET_HIVE_SERDE_CLASS, HUDI_PARQUET_INPUT_FORMAT, MAPRED_PARQUET_OUTPUT_FORMAT_CLASS))
                        .setLocation(basePath))
                .build();

        List<HiveColumnHandle> partitionColumnHandles = getPartitionKeyColumnHandles(table, TESTING_TYPE_MANAGER);
        Map<String, HiveColumnHandle> partitionColumnHandleMap = partitionColumnHandles.stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));

        HudiTableHandle tableHandle = new HudiTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                basePath,
                COPY_ON_WRITE,
                partitionColumnHandles,
                TupleDomain.all(),
                TupleDomain.all());

        ExecutorService executor = Executors.newCachedThreadPool();
        ScheduledExecutorService splitLoaderExecutorService = Executors.newScheduledThreadPool(2);
        try {
            HudiSplitSource splitSource = new HudiSplitSource(
                    session,
                    new PartitionNotFoundHiveMetastore(),
                    table,
                    tableHandle,
                    fileSystemFactory,
                    partitionColumnHandleMap,
                    executor,
                    splitLoaderExecutorService,
                    1000,
                    1000,
                    new NoopSplitAffinityProvider(),
                    Long.MAX_VALUE,
                    List.of(MISSING_PARTITION_NAME));
            try {
                // The split loader runs asynchronously, and the failure can be observed either from
                // isFinished() or getNextBatch() depending on scheduling; poll isFinished() alone,
                // without ever calling getNextBatch(), to exercise the specific path that silently
                // reported success.
                assertEventually(new Duration(30, SECONDS), () ->
                        assertThatThrownBy(splitSource::isFinished)
                                .isInstanceOf(TrinoException.class)
                                .satisfies(exception -> assertThat(((TrinoException) exception).getErrorCode()).isEqualTo(HUDI_CANNOT_OPEN_SPLIT.toErrorCode()))
                                .cause()
                                .isInstanceOf(TrinoException.class)
                                .satisfies(cause -> assertThat(((TrinoException) cause).getErrorCode()).isEqualTo(HUDI_PARTITION_NOT_FOUND.toErrorCode())));
            }
            finally {
                splitSource.close();
            }
        }
        finally {
            executor.shutdownNow();
            splitLoaderExecutorService.shutdownNow();
        }
    }

    /**
     * A hand-written fake, not a mock: every partition lookup fails as though the partition was
     * concurrently dropped from the metastore after being listed, which is what a real split-loader
     * failure looks like in production. Every other method is unused by this test.
     */
    private static class PartitionNotFoundHiveMetastore
            implements HiveMetastore
    {
        @Override
        public Optional<Partition> getPartition(Table table, List<String> partitionValues)
        {
            return Optional.empty();
        }

        @Override
        public Optional<Database> getDatabase(String databaseName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getAllDatabases()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Table> getTable(String databaseName, String tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<TableInfo> getTables(String databaseName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getTableNamesWithParameters(String databaseName, String parameterKey, Set<String> parameterValues)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createDatabase(Database database)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropDatabase(String databaseName, boolean deleteData)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameDatabase(String databaseName, String newDatabaseName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDatabaseOwner(String databaseName, HivePrincipal principal)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createTable(Table table, PrincipalPrivileges principalPrivileges)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropTable(String databaseName, String tableName, boolean deleteData)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Map<String, String> environmentContext)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void commentTable(String databaseName, String tableName, Optional<String> comment)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropColumn(String databaseName, String tableName, String columnName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createRole(String role, String grantor)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropRole(String role)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> listRoles()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean functionExists(String databaseName, String functionName, String signatureToken)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<LanguageFunction> getAllFunctions(String databaseName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createFunction(String databaseName, String functionName, LanguageFunction function)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropFunction(String databaseName, String functionName, String signatureToken)
        {
            throw new UnsupportedOperationException();
        }
    }
}
