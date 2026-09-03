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
package io.trino.plugin.hive.metastore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveBasicStatistics;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.Table;
import io.trino.plugin.hive.TableInvalidationCallback;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore.UpdateStatisticsOperation;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.metastore.StatisticsUpdateMode.MERGE_INCREMENTAL;
import static io.trino.metastore.StatisticsUpdateMode.OVERWRITE_ALL;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.file.Files.createTempDirectory;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSemiTransactionalHiveMetastore
{
    private static final SchemaTableName TABLE = new SchemaTableName("test_database", "test_table");
    private static final Location TABLE_LOCATION = Location.of("local:///data/test_table");
    private static final SchemaTableName PARTITIONED_TABLE = new SchemaTableName("test_database", "test_partitioned_table");
    private static final String PARTITION_NAME = "ds=1";
    private static final PartitionStatistics EXISTING_STATISTICS = new PartitionStatistics(
            new HiveBasicStatistics(1, 10, 100, 1000),
            ImmutableMap.of(
                    "a", createIntegerColumnStatistics(OptionalLong.of(1), OptionalLong.of(10), OptionalLong.of(0), OptionalLong.of(10)),
                    "b", createIntegerColumnStatistics(OptionalLong.of(5), OptionalLong.of(6), OptionalLong.of(1), OptionalLong.of(2))));
    private static final PartitionStatistics STATISTICS_UPDATE = new PartitionStatistics(
            new HiveBasicStatistics(1, 5, 50, 500),
            ImmutableMap.of("a", createIntegerColumnStatistics(OptionalLong.of(20), OptionalLong.of(30), OptionalLong.of(0), OptionalLong.of(5))));

    private Path tempDir;
    private ScheduledExecutorService heartbeatService;
    private PartiallyFailingMetastore metastore;
    private SemiTransactionalHiveMetastore transactionalMetastore;

    @BeforeEach
    void setUp()
            throws IOException
    {
        tempDir = createTempDirectory("test");
        Files.createDirectories(tempDir.resolve("data").resolve(TABLE.getTableName()));
        Files.createDirectories(tempDir.resolve("data").resolve(PARTITION_NAME));
        heartbeatService = newSingleThreadScheduledExecutor();
        LocalFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(tempDir);
        metastore = new PartiallyFailingMetastore(fileSystemFactory);
        metastore.createDatabase(Database.builder()
                .setDatabaseName(TABLE.getSchemaName())
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty())
                .build());
        createTable(TABLE, ImmutableList.of());
        metastore.updateTableStatistics(TABLE.getSchemaName(), TABLE.getTableName(), OptionalLong.empty(), OVERWRITE_ALL, EXISTING_STATISTICS);

        createTable(PARTITIONED_TABLE, ImmutableList.of(new Column("ds", HIVE_INT, Optional.empty(), Map.of())));
        Partition partition = Partition.builder()
                .setDatabaseName(PARTITIONED_TABLE.getSchemaName())
                .setTableName(PARTITIONED_TABLE.getTableName())
                .setValues(ImmutableList.of("1"))
                .setColumns(dataColumns())
                .setParameters(ImmutableMap.of())
                .withStorage(storage -> storage
                        .setStorageFormat(ORC.toStorageFormat())
                        .setLocation("local:///data/" + PARTITION_NAME))
                .build();
        metastore.addPartitions(PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName(), ImmutableList.of(new PartitionWithStatistics(partition, PARTITION_NAME, EXISTING_STATISTICS)));

        transactionalMetastore = new SemiTransactionalHiveMetastore(
                TESTING_TYPE_MANAGER,
                false,
                fileSystemFactory,
                metastore,
                directExecutor(),
                directExecutor(),
                directExecutor(),
                false,
                false,
                false,
                Optional.empty(),
                heartbeatService,
                new TableInvalidationCallback() {});
        transactionalMetastore.beginQuery(SESSION);
    }

    @AfterEach
    void tearDown()
            throws IOException
    {
        heartbeatService.shutdownNow();
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Test
    void testCommitMergesStatistics()
    {
        insertIntoExistingTable();
        transactionalMetastore.commit();
        assertThat(storedStatistics()).isEqualTo(MERGE_INCREMENTAL.updatePartitionStatistics(EXISTING_STATISTICS, STATISTICS_UPDATE));
    }

    @Test
    void testFailedCommitRestoresStatistics()
    {
        insertIntoExistingTable();
        metastore.failAfterNextUpdate = true;
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(storedStatistics()).isEqualTo(EXISTING_STATISTICS);
    }

    @Test
    void testCommitRejectsConcurrentColumnChange()
    {
        Table table = metastore.getTable(TABLE.getSchemaName(), TABLE.getTableName()).orElseThrow();
        transactionalMetastore.dropTable(SESSION, TABLE.getSchemaName(), TABLE.getTableName());
        transactionalMetastore.createTable(SESSION, table, NO_PRIVILEGES, Optional.of(TABLE_LOCATION), Optional.of(ImmutableList.of()), false, STATISTICS_UPDATE, false);

        Table tableWithExtraColumn = Table.builder(table)
                .setDataColumns(ImmutableList.<Column>builder()
                        .addAll(dataColumns())
                        .add(new Column("c", HIVE_INT, Optional.empty(), Map.of()))
                        .build())
                .build();
        metastore.replaceTable(TABLE.getSchemaName(), TABLE.getTableName(), tableWithExtraColumn, NO_PRIVILEGES, ImmutableMap.of());

        assertTrinoExceptionThrownBy(transactionalMetastore::commit).hasErrorCode(TRANSACTION_CONFLICT);
    }

    @Test
    void testUndoRestoresStatisticsAfterMerge()
    {
        UpdateStatisticsOperation operation = tableOperation(EXISTING_STATISTICS, MERGE_INCREMENTAL);
        operation.run(metastore, NO_ACID_TRANSACTION);
        assertThat(storedStatistics()).isEqualTo(MERGE_INCREMENTAL.updatePartitionStatistics(EXISTING_STATISTICS, STATISTICS_UPDATE));

        operation.undo(metastore, NO_ACID_TRANSACTION);
        assertThat(storedStatistics()).isEqualTo(EXISTING_STATISTICS);
    }

    @Test
    void testUndoRestoresDroppedColumnStatisticsAfterOverwrite()
    {
        UpdateStatisticsOperation operation = tableOperation(EXISTING_STATISTICS, OVERWRITE_ALL);
        operation.run(metastore, NO_ACID_TRANSACTION);
        assertThat(storedStatistics()).isEqualTo(STATISTICS_UPDATE);

        operation.undo(metastore, NO_ACID_TRANSACTION);
        assertThat(storedStatistics()).isEqualTo(EXISTING_STATISTICS);
    }

    @Test
    void testUndoRestoresPartitionStatisticsAfterMerge()
    {
        UpdateStatisticsOperation operation = new UpdateStatisticsOperation(PARTITIONED_TABLE, Optional.of(PARTITION_NAME), EXISTING_STATISTICS, STATISTICS_UPDATE, MERGE_INCREMENTAL);
        operation.run(metastore, NO_ACID_TRANSACTION);
        assertThat(storedPartitionStatistics()).isEqualTo(MERGE_INCREMENTAL.updatePartitionStatistics(EXISTING_STATISTICS, STATISTICS_UPDATE));

        operation.undo(metastore, NO_ACID_TRANSACTION);
        assertThat(storedPartitionStatistics()).isEqualTo(EXISTING_STATISTICS);
    }

    @Test
    void testUndoWithoutRunWritesNothing()
    {
        UpdateStatisticsOperation operation = tableOperation(STATISTICS_UPDATE, OVERWRITE_ALL);
        operation.undo(metastore, NO_ACID_TRANSACTION);
        assertThat(storedStatistics()).isEqualTo(EXISTING_STATISTICS);
    }

    @Test
    void testUndoAttemptsRestoreAfterFailedRun()
    {
        UpdateStatisticsOperation operation = new UpdateStatisticsOperation(
                new SchemaTableName(TABLE.getSchemaName(), "missing"),
                Optional.empty(),
                EXISTING_STATISTICS,
                STATISTICS_UPDATE,
                OVERWRITE_ALL);
        assertThatThrownBy(() -> operation.run(metastore, NO_ACID_TRANSACTION)).isInstanceOf(TableNotFoundException.class);
        assertThatThrownBy(() -> operation.undo(metastore, NO_ACID_TRANSACTION)).isInstanceOf(TableNotFoundException.class);
    }

    private void insertIntoExistingTable()
    {
        transactionalMetastore.finishChangingExistingTable(
                AcidOperation.INSERT,
                SESSION,
                TABLE.getSchemaName(),
                TABLE.getTableName(),
                TABLE_LOCATION,
                ImmutableList.of(),
                STATISTICS_UPDATE,
                false);
    }

    private void createTable(SchemaTableName tableName, List<Column> partitionColumns)
    {
        metastore.createTable(
                Table.builder()
                        .setDatabaseName(tableName.getSchemaName())
                        .setTableName(tableName.getTableName())
                        .setTableType(EXTERNAL_TABLE.name())
                        .setOwner(Optional.of("test"))
                        .setDataColumns(dataColumns())
                        .setPartitionColumns(partitionColumns)
                        .setParameters(ImmutableMap.of("EXTERNAL", "TRUE"))
                        .withStorage(storage -> storage
                                .setStorageFormat(ORC.toStorageFormat())
                                .setLocation("local:///data/" + tableName.getTableName()))
                        .build(),
                NO_PRIVILEGES);
    }

    private static List<Column> dataColumns()
    {
        return ImmutableList.of(
                new Column("a", HIVE_INT, Optional.empty(), Map.of()),
                new Column("b", HIVE_INT, Optional.empty(), Map.of()));
    }

    private static UpdateStatisticsOperation tableOperation(PartitionStatistics statisticsBeforeUpdate, StatisticsUpdateMode mode)
    {
        return new UpdateStatisticsOperation(TABLE, Optional.empty(), statisticsBeforeUpdate, STATISTICS_UPDATE, mode);
    }

    private PartitionStatistics storedStatistics()
    {
        Table table = metastore.getTable(TABLE.getSchemaName(), TABLE.getTableName()).orElseThrow();
        return new PartitionStatistics(
                getHiveBasicStatistics(table.getParameters()),
                metastore.getTableColumnStatistics(TABLE.getSchemaName(), TABLE.getTableName(), ImmutableSet.of("a", "b")));
    }

    private PartitionStatistics storedPartitionStatistics()
    {
        Table table = metastore.getTable(PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName()).orElseThrow();
        Partition partition = metastore.getPartition(table, ImmutableList.of("1")).orElseThrow();
        return new PartitionStatistics(
                getHiveBasicStatistics(partition.getParameters()),
                metastore.getPartitionColumnStatistics(PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName(), ImmutableSet.of(PARTITION_NAME), ImmutableSet.of("a", "b")).get(PARTITION_NAME));
    }

    // applies the update, then reports it as failed, like a metastore response lost after the write
    private static class PartiallyFailingMetastore
            extends FileHiveMetastore
    {
        private boolean failAfterNextUpdate;

        PartiallyFailingMetastore(LocalFileSystemFactory fileSystemFactory)
        {
            super(new NodeVersion("testversion"),
                    fileSystemFactory,
                    false,
                    new FileHiveMetastoreConfig()
                            .setCatalogDirectory("local:///metastore")
                            .setMetastoreUser("test")
                            .setDisableLocationChecks(true));
        }

        @Override
        public synchronized void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
        {
            super.updateTableStatistics(databaseName, tableName, acidWriteId, mode, statisticsUpdate);
            if (failAfterNextUpdate) {
                failAfterNextUpdate = false;
                throw new TrinoException(HIVE_METASTORE_ERROR, "statistics update response lost");
            }
        }
    }
}
