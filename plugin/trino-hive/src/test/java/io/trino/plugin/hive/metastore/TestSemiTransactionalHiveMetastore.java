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
import io.trino.filesystem.ForwardingTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveBasicStatistics;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveLocationService;
import io.trino.plugin.hive.LocationHandle;
import io.trino.plugin.hive.LocationService.WriteInfo;
import io.trino.plugin.hive.TableInvalidationCallback;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore.PartitionUpdateInfo;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore.UpdateStatisticsBatch;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore.UpdateStatisticsOperation;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.Partitions.toPartitionValues;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.metastore.StatisticsUpdateMode.MERGE_INCREMENTAL;
import static io.trino.metastore.StatisticsUpdateMode.OVERWRITE_ALL;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static io.trino.plugin.hive.LocationHandle.WriteMode.STAGE_AND_MOVE_FILES_TO_TARGET_DIRECTORY;
import static io.trino.plugin.hive.LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSemiTransactionalHiveMetastore
{
    private static final SchemaTableName TABLE = new SchemaTableName("test_database", "test_table");
    private static final Location TABLE_LOCATION = Location.of("local:///data/test_table");
    private static final SchemaTableName PARTITIONED_TABLE = new SchemaTableName("test_database", "test_partitioned_table");
    private static final Location CTAS_LOCATION = Location.of("local:///metastore/test_database/ctas_table");
    private static final String PARTITION_NAME = "ds=1";
    private static final String SECOND_PARTITION_NAME = "ds=2";
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
    private BoundaryFileSystem boundaryFileSystem;

    @BeforeEach
    void setUp()
            throws IOException
    {
        tempDir = createTempDirectory("test");
        Files.createDirectories(tempDir.resolve("data").resolve(TABLE.getTableName()));
        Files.createDirectories(tempDir.resolve("data").resolve(PARTITION_NAME));
        Files.createDirectories(tempDir.resolve("data").resolve(SECOND_PARTITION_NAME));
        heartbeatService = newSingleThreadScheduledExecutor();
        LocalFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(tempDir);
        metastore = new PartiallyFailingMetastore(fileSystemFactory);
        boundaryFileSystem = new BoundaryFileSystem(new LocalFileSystem(tempDir));
        metastore.createDatabase(Database.builder()
                .setDatabaseName(TABLE.getSchemaName())
                .setLocation(Optional.of("local:///metastore/test_database"))
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty())
                .build());
        Files.createDirectories(path(CTAS_LOCATION.parentDirectory()));
        createTable(TABLE, ImmutableList.of());
        metastore.updateTableStatistics(TABLE.getSchemaName(), TABLE.getTableName(), OptionalLong.empty(), OVERWRITE_ALL, EXISTING_STATISTICS);

        createTable(PARTITIONED_TABLE, ImmutableList.of(new Column("ds", HIVE_INT, Optional.empty(), Map.of())));
        metastore.addPartitions(PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName(), ImmutableList.of(
                partitionWithStatistics("1", PARTITION_NAME),
                partitionWithStatistics("2", SECOND_PARTITION_NAME)));

        beginTransaction();
    }

    private void beginTransaction()
    {
        transactionalMetastore = new SemiTransactionalHiveMetastore(
                TESTING_TYPE_MANAGER,
                false,
                _ -> boundaryFileSystem,
                metastore,
                directExecutor(),
                directExecutor(),
                directExecutor(),
                2,
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
    void testFailedCommitRestoresPartitionStatistics()
    {
        transactionalMetastore.finishInsertIntoExistingPartitions(
                SESSION,
                PARTITIONED_TABLE.getSchemaName(),
                PARTITIONED_TABLE.getTableName(),
                ImmutableList.of(partitionUpdate("1", PARTITION_NAME), partitionUpdate("2", SECOND_PARTITION_NAME)),
                false);
        metastore.failAfterNextUpdate = true;
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(storedPartitionStatistics(PARTITION_NAME)).isEqualTo(EXISTING_STATISTICS);
        assertThat(storedPartitionStatistics(SECOND_PARTITION_NAME)).isEqualTo(EXISTING_STATISTICS);
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
        UpdateStatisticsOperation operation = partitionOperation(PARTITION_NAME, MERGE_INCREMENTAL);
        operation.run(metastore, NO_ACID_TRANSACTION);
        assertThat(storedPartitionStatistics(PARTITION_NAME)).isEqualTo(MERGE_INCREMENTAL.updatePartitionStatistics(EXISTING_STATISTICS, STATISTICS_UPDATE));

        operation.undo(metastore, NO_ACID_TRANSACTION);
        assertThat(storedPartitionStatistics(PARTITION_NAME)).isEqualTo(EXISTING_STATISTICS);
    }

    @Test
    void testBatchesGroupPartitionsByTableAndMode()
    {
        UpdateStatisticsOperation tableOperation = tableOperation(EXISTING_STATISTICS, MERGE_INCREMENTAL);
        UpdateStatisticsOperation firstMerge = partitionOperation("ds=1", MERGE_INCREMENTAL);
        UpdateStatisticsOperation overwrite = partitionOperation("ds=2", OVERWRITE_ALL);
        UpdateStatisticsOperation secondMerge = partitionOperation("ds=3", MERGE_INCREMENTAL);
        UpdateStatisticsOperation thirdMerge = partitionOperation("ds=4", MERGE_INCREMENTAL);

        List<UpdateStatisticsBatch> batches = UpdateStatisticsBatch.createBatches(ImmutableList.of(tableOperation, firstMerge, overwrite, secondMerge, thirdMerge), 2);

        assertThat(batches).map(UpdateStatisticsBatch::getOperations).containsExactly(
                ImmutableList.of(tableOperation),
                ImmutableList.of(firstMerge, secondMerge),
                ImmutableList.of(thirdMerge),
                ImmutableList.of(overwrite));
        assertThat(batches.get(1).getDescription()).isEqualTo("replace partition parameters test_database.test_partitioned_table ds=1; replace partition parameters test_database.test_partitioned_table ds=3");
    }

    @Test
    void testBatchUndoRestoresAllPartitionsAfterMerge()
    {
        UpdateStatisticsBatch batch = partitionBatch(MERGE_INCREMENTAL);
        batch.run(metastore, NO_ACID_TRANSACTION);
        PartitionStatistics merged = MERGE_INCREMENTAL.updatePartitionStatistics(EXISTING_STATISTICS, STATISTICS_UPDATE);
        assertThat(storedPartitionStatistics(PARTITION_NAME)).isEqualTo(merged);
        assertThat(storedPartitionStatistics(SECOND_PARTITION_NAME)).isEqualTo(merged);

        batch.undo(metastore, NO_ACID_TRANSACTION);
        assertThat(storedPartitionStatistics(PARTITION_NAME)).isEqualTo(EXISTING_STATISTICS);
        assertThat(storedPartitionStatistics(SECOND_PARTITION_NAME)).isEqualTo(EXISTING_STATISTICS);
    }

    @Test
    void testBatchUndoWithoutRunWritesNothing()
    {
        UpdateStatisticsBatch batch = partitionBatch(OVERWRITE_ALL);
        batch.undo(metastore, NO_ACID_TRANSACTION);
        assertThat(storedPartitionStatistics(PARTITION_NAME)).isEqualTo(EXISTING_STATISTICS);
        assertThat(storedPartitionStatistics(SECOND_PARTITION_NAME)).isEqualTo(EXISTING_STATISTICS);
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

    @Test
    void testTableLocalAppend()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        Location staging = declareLocalWrite(TABLE, TABLE_LOCATION);
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        writeFile(TABLE_LOCATION.appendPath("existing"));
        Location otherStaging = TABLE_LOCATION.appendPath(".hive-staging/other-query/data");
        writeFile(otherStaging);
        finishLocalTableInsert(staging, List.of(file));
        transactionalMetastore.commit();
        assertThat(path(TABLE_LOCATION.appendPath(file))).exists();
        assertThat(path(TABLE_LOCATION.appendPath("existing"))).exists();
        assertThat(path(otherStaging)).exists();
        assertThat(path(staging)).doesNotExist();
        assertThat(boundaryFileSystem.renames).containsExactly(new FileRename(staging.appendPath(file), TABLE_LOCATION.appendPath(file)));
    }

    @Test
    void testTableLocalCustomPartitionAppend()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        Location tableLocation = Location.of("local:///data/test_partitioned_table");
        declareLocalWrite(PARTITIONED_TABLE, tableLocation);
        // This partition's custom location is outside the table directory.
        Location target = Location.of("local:///data/" + PARTITION_NAME);
        Location staging = target.appendPath(".hive-staging/operation");
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        writeFile(target.appendPath("existing"));
        transactionalMetastore.finishInsertIntoExistingPartitions(
                SESSION,
                PARTITIONED_TABLE.getSchemaName(),
                PARTITIONED_TABLE.getTableName(),
                List.of(new PartitionUpdateInfo(List.of("1"), staging, List.of(file), STATISTICS_UPDATE, MERGE_INCREMENTAL)),
                false);
        transactionalMetastore.commit();
        assertThat(path(target.appendPath(file))).exists();
        assertThat(path(target.appendPath("existing"))).exists();
        assertThat(path(staging)).doesNotExist();
        assertThat(boundaryFileSystem.renames).containsExactly(new FileRename(staging.appendPath(file), target.appendPath(file)));
    }

    @Test
    void testTableLocalOrdinaryPartitionAppend()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        Location tableRoot = Location.of("local:///data/test_partitioned_table");
        Location target = tableRoot.appendPath(PARTITION_NAME);
        Files.createDirectories(path(target));
        Partition partition = Partition.builder(partitionWithStatistics("1", PARTITION_NAME).getPartition())
                .withStorage(storage -> storage.setLocation(target.toString()))
                .build();
        metastore.alterPartition(PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName(), new PartitionWithStatistics(partition, PARTITION_NAME, EXISTING_STATISTICS));
        Location staging = declareLocalWrite(PARTITIONED_TABLE, tableRoot).appendPath(PARTITION_NAME);
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        writeFile(target.appendPath("existing"));
        transactionalMetastore.finishInsertIntoExistingPartitions(
                SESSION,
                PARTITIONED_TABLE.getSchemaName(),
                PARTITIONED_TABLE.getTableName(),
                List.of(new PartitionUpdateInfo(List.of("1"), staging, List.of(file), STATISTICS_UPDATE, MERGE_INCREMENTAL)),
                false);
        transactionalMetastore.commit();
        assertThat(path(target.appendPath(file))).exists();
        assertThat(path(target.appendPath("existing"))).exists();
        assertThat(path(staging)).doesNotExist();
        assertThat(boundaryFileSystem.renames).containsExactly(new FileRename(staging.appendPath(file), target.appendPath(file)));
    }

    @Test
    void testTableLocalUnreportedOrdinaryPartitionRollbackAndRetry()
            throws IOException
    {
        Location tableRoot = Location.of("local:///data/test_partitioned_table");
        Location staging = declareLocalWrite(PARTITIONED_TABLE, tableRoot);
        Location unreported = staging.appendPath("ds=3/" + SESSION.getQueryId() + "_unreported");
        Location other = tableRoot.appendPath(".hive-staging/other-query/data");
        writeFile(unreported);
        writeFile(other);
        transactionalMetastore.rollback();
        assertThat(path(staging)).doesNotExist();
        assertThat(path(other)).exists();

        beginTransaction();
        Location retryStaging = declareLocalWrite(PARTITIONED_TABLE, tableRoot).appendPath("ds=3");
        String file = SESSION.getQueryId() + "_retry";
        writeFile(retryStaging.appendPath(file));
        transactionalMetastore.addPartition(SESSION, PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName(), newLocalPartition(), retryStaging, Optional.of(List.of(file)), STATISTICS_UPDATE, false);
        transactionalMetastore.commit();
        assertThat(path(tableRoot.appendPath("ds=3").appendPath(file))).exists();
        assertThat(path(staging)).doesNotExist();
        assertThat(path(other)).exists();
    }

    @Test
    void testTableLocalNewPartition()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        Partition partition = newLocalPartition();
        Location target = Location.of(partition.getStorage().getLocation());
        Location staging = declareLocalWrite(PARTITIONED_TABLE, Location.of("local:///data/test_partitioned_table"));
        Location partitionStaging = staging.appendPath("ds=3");
        String file = SESSION.getQueryId() + "_new";
        writeFile(partitionStaging.appendPath(file));
        String unreported = SESSION.getQueryId() + "_unreported";
        writeFile(partitionStaging.appendPath(unreported));
        Location other = staging.parentDirectory().appendPath("other-query/data");
        writeFile(other);
        transactionalMetastore.addPartition(
                SESSION,
                PARTITIONED_TABLE.getSchemaName(),
                PARTITIONED_TABLE.getTableName(),
                partition,
                partitionStaging,
                Optional.of(List.of(file)),
                STATISTICS_UPDATE,
                false);
        transactionalMetastore.commit();
        assertThat(path(target.appendPath(file))).exists();
        assertThat(path(target.appendPath(unreported))).doesNotExist();
        assertThat(path(other)).exists();
        assertThat(path(partitionStaging)).doesNotExist();
        assertThat(path(staging)).doesNotExist();
        Table table = metastore.getTable(PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName()).orElseThrow();
        assertThat(metastore.getPartition(table, List.of("3"))).isPresent();
        assertThat(boundaryFileSystem.directoryRenames).containsExactly(new FileRename(partitionStaging, target));
        assertThat(boundaryFileSystem.renames).isEmpty();
    }

    @Test
    void testTableLocalNewPartitionPreservesExistingDirectory()
            throws IOException
    {
        Location staging = declareLocalWrite(PARTITIONED_TABLE, Location.of("local:///data/test_partitioned_table")).appendPath("ds=3");
        Partition partition = newLocalPartition();
        Location target = Location.of(partition.getStorage().getLocation());
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        writeFile(target.appendPath("foreign"));
        transactionalMetastore.addPartition(SESSION, PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName(), partition, staging, Optional.of(List.of(file)), STATISTICS_UPDATE, false);
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(path(target.appendPath("foreign"))).exists();
        assertThat(path(target.appendPath(file))).doesNotExist();
        assertThat(path(staging)).doesNotExist();
    }

    @Test
    void testTableLocalPartialPromotionFailure()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        boundaryFileSystem.failRenameNumber = 2;
        Location staging = declareLocalWrite(TABLE, TABLE_LOCATION);
        List<String> files = List.of(SESSION.getQueryId() + "_one", SESSION.getQueryId() + "_two");
        for (String file : files) {
            writeFile(staging.appendPath(file));
        }
        writeFile(TABLE_LOCATION.appendPath("foreign"));
        Location other = TABLE_LOCATION.appendPath(".hive-staging/other-query/data");
        writeFile(other);
        finishLocalTableInsert(staging, files);
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class).hasMessageContaining("Error moving data files");
        assertThat(boundaryFileSystem.renames).hasSize(2);
        for (String file : files) {
            assertThat(path(TABLE_LOCATION.appendPath(file))).doesNotExist();
        }
        assertThat(path(staging)).doesNotExist();
        assertThat(path(TABLE_LOCATION.appendPath("foreign"))).exists();
        assertThat(path(other)).exists();
        assertThat(storedStatistics()).isEqualTo(EXISTING_STATISTICS);
    }

    @Test
    void testTableLocalPromotionFailureAfterRename()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        boundaryFileSystem.failAfterRename = true;
        Location staging = declareLocalWrite(TABLE, TABLE_LOCATION);
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        finishLocalTableInsert(staging, List.of(file));
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(path(TABLE_LOCATION.appendPath(file))).doesNotExist();
        assertThat(path(staging)).doesNotExist();
    }

    @Test
    void testTableLocalPromotionFailureAndVerificationFailure()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        boundaryFileSystem.failAfterRename = true;
        boundaryFileSystem.failVerificationAfterRename = true;
        Location staging = declareLocalWrite(TABLE, TABLE_LOCATION);
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        Location unrelated = TABLE_LOCATION.appendPath("unrelated");
        writeFile(unrelated);
        finishLocalTableInsert(staging, List.of(file));
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(boundaryFileSystem.verificationFailures).isEqualTo(1);
        assertThat(path(TABLE_LOCATION.appendPath(file))).doesNotExist();
        assertThat(path(unrelated)).exists();
        assertThat(path(staging)).doesNotExist();
    }

    @Test
    void testTableLocalPromotionFailurePreservesConflictingDestination()
            throws IOException
    {
        boundaryFileSystem.createConflictingDestination = true;
        Location staging = declareLocalWrite(TABLE, TABLE_LOCATION);
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        finishLocalTableInsert(staging, List.of(file));
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(Files.readString(path(TABLE_LOCATION.appendPath(file)))).isEqualTo("conflicting destination");
        assertThat(path(staging)).doesNotExist();
    }

    @Test
    void testTableLocalPromotionPreservesRacingIdenticalDestination()
            throws IOException
    {
        boundaryFileSystem.failBeforeSourceRemoval = true;
        boundaryFileSystem.destinationAlreadyExists = true;
        Location staging = declareLocalWrite(TABLE, TABLE_LOCATION);
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        finishLocalTableInsert(staging, List.of(file));
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(path(TABLE_LOCATION.appendPath(file))).exists();
        assertThat(path(staging)).doesNotExist();
    }

    @Test
    void testTableLocalPromotionFailureLeavesBothPaths()
            throws IOException
    {
        boundaryFileSystem.failBeforeSourceRemoval = true;
        Location staging = declareLocalWrite(TABLE, TABLE_LOCATION);
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        finishLocalTableInsert(staging, List.of(file));
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(path(TABLE_LOCATION.appendPath(file))).doesNotExist();
        assertThat(path(staging)).doesNotExist();
    }

    @Test
    void testTableLocalPromotionPreservesExistingDestination()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        Location staging = declareLocalWrite(TABLE, TABLE_LOCATION);
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        Files.writeString(path(TABLE_LOCATION.appendPath(file)), "foreign");
        finishLocalTableInsert(staging, List.of(file));
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(Files.readString(path(TABLE_LOCATION.appendPath(file)))).isEqualTo("foreign");
        assertThat(path(staging)).doesNotExist();
    }

    @Test
    void testTableLocalMissingSourcePreservesDestination()
            throws IOException
    {
        Location staging = declareLocalWrite(TABLE, TABLE_LOCATION);
        String file = SESSION.getQueryId() + "_missing";
        Files.writeString(path(TABLE_LOCATION.appendPath(file)), "foreign");
        finishLocalTableInsert(staging, List.of(file));
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(Files.readString(path(TABLE_LOCATION.appendPath(file)))).isEqualTo("foreign");
        assertThat(path(staging)).doesNotExist();
    }

    @Test
    void testTableLocalMetadataFailure()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        Location staging = declareLocalWrite(TABLE, TABLE_LOCATION);
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        finishLocalTableInsert(staging, List.of(file));
        metastore.failAfterNextUpdate = true;
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(path(TABLE_LOCATION.appendPath(file))).doesNotExist();
        assertThat(path(staging)).doesNotExist();
        assertThat(storedStatistics()).isEqualTo(EXISTING_STATISTICS);
    }

    @Test
    void testTableLocalNewPartitionMetadataFailure()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        Partition partition = newLocalPartition();
        Location target = Location.of(partition.getStorage().getLocation());
        declareLocalWrite(PARTITIONED_TABLE, Location.of("local:///data/test_partitioned_table"));
        Location staging = Location.of("local:///data/test_partitioned_table/.hive-staging/operation/ds=3");
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        Location foreign = target.parentDirectory().appendPath("foreign");
        writeFile(foreign);
        transactionalMetastore.addPartition(
                SESSION,
                PARTITIONED_TABLE.getSchemaName(),
                PARTITIONED_TABLE.getTableName(),
                partition,
                staging,
                Optional.of(List.of(file)),
                STATISTICS_UPDATE,
                false);
        metastore.failNextPartitionAdd = true;
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(path(target.appendPath(file))).doesNotExist();
        assertThat(path(staging)).doesNotExist();
        assertThat(path(foreign)).exists();
        Table table = metastore.getTable(PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName()).orElseThrow();
        assertThat(metastore.getPartition(table, List.of("3"))).isEmpty();
    }

    @Test
    void testTableLocalNewPartitionRollbackPreservesOtherStagingTree()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        boundaryFileSystem.createOtherStagingAfterDirectoryRename = true;
        metastore.failNextPartitionAdd = true;
        Location staging = declareLocalWrite(PARTITIONED_TABLE, Location.of("local:///data/test_partitioned_table")).appendPath("ds=3");
        Partition partition = newLocalPartition();
        Location target = Location.of(partition.getStorage().getLocation());
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        transactionalMetastore.addPartition(SESSION, PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName(), partition, staging, Optional.of(List.of(file)), STATISTICS_UPDATE, false);
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(path(target.appendPath(file))).doesNotExist();
        assertThat(path(target.appendPath(".hive-staging/other-operation"))).isDirectory();
        assertThat(path(staging)).doesNotExist();
    }

    @Test
    void testTableLocalNewPartitionFailureAfterDirectoryRename()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        boundaryFileSystem.failAfterDirectoryRename = true;
        Location staging = declareLocalWrite(PARTITIONED_TABLE, Location.of("local:///data/test_partitioned_table")).appendPath("ds=3");
        Partition partition = newLocalPartition();
        Location target = Location.of(partition.getStorage().getLocation());
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        transactionalMetastore.addPartition(SESSION, PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName(), partition, staging, Optional.of(List.of(file)), STATISTICS_UPDATE, false);
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(path(target)).doesNotExist();
        assertThat(path(staging)).doesNotExist();
        assertThat(boundaryFileSystem.directoryRenames).containsExactly(new FileRename(staging, target));
    }

    @Test
    void testTableLocalRollbackPreservesForeignFiles()
            throws IOException
    {
        Location staging = declareLocalWrite(TABLE, TABLE_LOCATION);
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        writeFile(staging.appendPath("foreign"));
        Location other = TABLE_LOCATION.appendPath(".hive-staging/other-query/data");
        writeFile(other);
        transactionalMetastore.rollback();
        assertThat(path(staging.appendPath(file))).doesNotExist();
        assertThat(path(staging.appendPath("foreign"))).exists();
        assertThat(path(other)).exists();
        assertThat(boundaryFileSystem.renames).isEmpty();
    }

    @Test
    void testTableLocalRollbackCustomPartition()
            throws IOException
    {
        declareLocalWrite(PARTITIONED_TABLE, Location.of("local:///data/test_partitioned_table"));
        Location target = Location.of("local:///data/" + PARTITION_NAME);
        Location staging = target.appendPath(".hive-staging/operation");
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        transactionalMetastore.finishInsertIntoExistingPartitions(
                SESSION,
                PARTITIONED_TABLE.getSchemaName(),
                PARTITIONED_TABLE.getTableName(),
                List.of(new PartitionUpdateInfo(List.of("1"), staging, List.of(file), STATISTICS_UPDATE, MERGE_INCREMENTAL)),
                false);
        transactionalMetastore.rollback();
        assertThat(path(staging)).doesNotExist();
        assertThat(path(target)).exists();
    }

    @Test
    void testTableLocalRejectsChangedPartitionBoundary()
            throws IOException
    {
        declareLocalWrite(PARTITIONED_TABLE, Location.of("local:///data/test_partitioned_table"));
        Location originalTarget = Location.of("local:///data/" + PARTITION_NAME);
        Location staging = originalTarget.appendPath(".hive-staging/operation");
        transactionalMetastore.registerTableLocalStagingDirectory(SESSION, PARTITIONED_TABLE, originalTarget, staging);
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        Partition changed = Partition.builder(partitionWithStatistics("1", PARTITION_NAME).getPartition())
                .withStorage(storage -> storage.setLocation("local:///data/changed-partition"))
                .build();
        Files.createDirectories(path(Location.of(changed.getStorage().getLocation())));
        metastore.alterPartition(PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName(), new PartitionWithStatistics(changed, PARTITION_NAME, EXISTING_STATISTICS));
        assertTrinoExceptionThrownBy(() -> transactionalMetastore.finishInsertIntoExistingPartitions(
                SESSION,
                PARTITIONED_TABLE.getSchemaName(),
                PARTITIONED_TABLE.getTableName(),
                List.of(new PartitionUpdateInfo(List.of("1"), staging, List.of(file), STATISTICS_UPDATE, MERGE_INCREMENTAL)),
                false))
                .hasMessage("Table or partition location changed during table-local staged insert");
        transactionalMetastore.rollback();
        assertThat(path(staging)).doesNotExist();
        assertThat(boundaryFileSystem.renames).isEmpty();
    }

    @Test
    void testTableLocalEmptyNewPartition()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        declareLocalWrite(PARTITIONED_TABLE, Location.of("local:///data/test_partitioned_table"));
        Partition partition = newLocalPartition();
        Location target = Location.of(partition.getStorage().getLocation());
        transactionalMetastore.addPartition(
                SESSION,
                PARTITIONED_TABLE.getSchemaName(),
                PARTITIONED_TABLE.getTableName(),
                partition,
                Location.of("local:///data/test_partitioned_table/.hive-staging/operation/ds=3"),
                Optional.of(List.of()),
                STATISTICS_UPDATE,
                false);
        transactionalMetastore.commit();
        assertThat(path(target)).isDirectory();
        assertThat(boundaryFileSystem.renames).isEmpty();
        Table table = metastore.getTable(PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName()).orElseThrow();
        assertThat(metastore.getPartition(table, List.of("3"))).isPresent();
    }

    @Test
    void testTableLocalUnreportedCustomPartitionStagingRequiresManualCleanup()
            throws IOException
    {
        declareLocalWrite(PARTITIONED_TABLE, Location.of("local:///data/test_partitioned_table"));
        Location unreported = Location.of("local:///data/" + PARTITION_NAME + "/.hive-staging/operation/" + SESSION.getQueryId() + "_unreported");
        writeFile(unreported);
        // A worker died before reporting this location. Cleanup must not scan every partition to find it.
        transactionalMetastore.rollback();
        assertThat(path(unreported)).exists();
    }

    @Test
    void testTableLocalRejectsDropThenInsertOverwrite()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        declareLocalWrite(PARTITIONED_TABLE, Location.of("local:///data/test_partitioned_table"));
        Partition partition = Partition.builder(partitionWithStatistics("1", PARTITION_NAME).getPartition())
                .setParameters(Map.of(TRINO_QUERY_ID_NAME, SESSION.getQueryId()))
                .build();
        Location target = Location.of(partition.getStorage().getLocation());
        Location staging = target.appendPath(".hive-staging/operation");
        String file = SESSION.getQueryId() + "_new";
        writeFile(staging.appendPath(file));
        writeFile(target.appendPath("existing"));
        transactionalMetastore.dropPartition(SESSION, PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName(), List.of("1"), true);
        transactionalMetastore.addPartition(
                SESSION,
                PARTITIONED_TABLE.getSchemaName(),
                PARTITIONED_TABLE.getTableName(),
                partition,
                staging,
                Optional.of(List.of(file)),
                STATISTICS_UPDATE,
                false);
        assertTrinoExceptionThrownBy(transactionalMetastore::commit).hasMessage("Overwrite is not supported with table-local staging");
        assertThat(path(target.appendPath("existing"))).exists();
        assertThat(path(staging)).doesNotExist();
        assertThat(boundaryFileSystem.renames).isEmpty();
    }

    @Test
    void testBoundaryFileSystemRejectsInvalidRenames()
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        Location nested = Location.of("local:///data/test_partitioned_table/ds=3");
        assertThatThrownBy(() -> boundaryFileSystem.renameDirectory(TABLE_LOCATION, Location.of("local:///backup"))).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> boundaryFileSystem.renameDirectory(nested, nested.appendPath("backup"))).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> boundaryFileSystem.renameFile(TABLE_LOCATION.appendPath("data"), nested.appendPath("data"))).isInstanceOf(IOException.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTableLocalCtas(boolean partitioned)
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        String file = SESSION.getQueryId() + "_new";
        Location staging = prepareLocalCtas(partitioned, List.of(file));
        Location source = partitioned ? staging.appendPath("ds=3") : staging;
        Location target = partitioned ? CTAS_LOCATION.appendPath("ds=3") : CTAS_LOCATION;
        writeFile(source.appendPath(file));
        writeFile(CTAS_LOCATION.appendPath("unrelated"));
        Location other = CTAS_LOCATION.appendPath(".hive-staging/other-operation/data");
        writeFile(other);
        if (partitioned) {
            addCtasPartition(source, List.of(file));
        }
        transactionalMetastore.commit();
        assertThat(path(target.appendPath(file))).exists();
        assertThat(path(CTAS_LOCATION.appendPath("unrelated"))).exists();
        assertThat(path(other)).exists();
        assertThat(path(staging)).doesNotExist();
        Table table = metastore.getTable(TABLE.getSchemaName(), "ctas_table").orElseThrow();
        assertThat(table.getTableType()).isEqualTo(MANAGED_TABLE.name());
        if (partitioned) {
            assertThat(boundaryFileSystem.directoryRenames).containsExactly(new FileRename(source, target));
            assertThat(boundaryFileSystem.renames).isEmpty();
            assertThat(metastore.getPartition(table, List.of("3"))).isPresent();
        }
        else {
            assertThat(boundaryFileSystem.renames).containsExactly(new FileRename(source.appendPath(file), target.appendPath(file)));
            assertThat(boundaryFileSystem.directoryRenames).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTableLocalEmptyCtas(boolean partitioned)
    {
        boundaryFileSystem.enforceBoundaries = true;
        Location staging = prepareLocalCtas(partitioned, List.of());
        transactionalMetastore.commit();
        assertThat(path(CTAS_LOCATION)).isDirectory();
        assertThat(path(staging)).doesNotExist();
        assertThat(metastore.getTable(TABLE.getSchemaName(), "ctas_table")).isPresent();
        assertThat(boundaryFileSystem.renames).isEmpty();
        assertThat(boundaryFileSystem.directoryRenames).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTableLocalCtasRollbackAndRetry(boolean partitioned)
            throws IOException
    {
        Location staging = prepareLocalCtas(partitioned, List.of());
        writeFile(staging.appendPath("ds=3/" + SESSION.getQueryId() + "_unreported"));
        transactionalMetastore.rollback();
        assertThat(path(CTAS_LOCATION)).doesNotExist();
        assertThat(metastore.getTable(TABLE.getSchemaName(), "ctas_table")).isEmpty();
        beginTransaction();
        prepareLocalCtas(partitioned, List.of());
        transactionalMetastore.commit();
        assertThat(path(CTAS_LOCATION)).isDirectory();
        assertThat(metastore.getTable(TABLE.getSchemaName(), "ctas_table")).isPresent();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTableLocalCtasPromotionFailure(boolean partitioned)
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        boundaryFileSystem.failAfterRename = !partitioned;
        boundaryFileSystem.failVerificationAfterRename = !partitioned;
        boundaryFileSystem.failAfterDirectoryRename = partitioned;
        String file = SESSION.getQueryId() + "_new";
        Location staging = prepareLocalCtas(partitioned, List.of(file));
        Location source = partitioned ? staging.appendPath("ds=3") : staging;
        writeFile(source.appendPath(file));
        if (partitioned) {
            addCtasPartition(source, List.of(file));
        }
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(path(CTAS_LOCATION)).doesNotExist();
        assertThat(metastore.getTable(TABLE.getSchemaName(), "ctas_table")).isEmpty();
        boundaryFileSystem.failAfterDirectoryRename = false;
        beginTransaction();
        prepareLocalCtas(partitioned, List.of());
        transactionalMetastore.commit();
        assertThat(metastore.getTable(TABLE.getSchemaName(), "ctas_table")).isPresent();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTableLocalCtasMetadataFailure(boolean partitioned)
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        String file = SESSION.getQueryId() + "_new";
        Location staging = prepareLocalCtas(partitioned, List.of(file));
        Location source = partitioned ? staging.appendPath("ds=3") : staging;
        writeFile(source.appendPath(file));
        if (partitioned) {
            addCtasPartition(source, List.of(file));
        }
        metastore.failAfterNextUpdate = true;
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(path(CTAS_LOCATION)).doesNotExist();
        assertThat(metastore.getTable(TABLE.getSchemaName(), "ctas_table")).isEmpty();
        beginTransaction();
        prepareLocalCtas(partitioned, List.of());
        transactionalMetastore.commit();
        assertThat(metastore.getTable(TABLE.getSchemaName(), "ctas_table")).isPresent();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTableLocalEmptyCtasCreateFailure(boolean partitioned)
    {
        prepareLocalCtas(partitioned, List.of());
        metastore.failNextTableCreate = true;
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(path(CTAS_LOCATION)).doesNotExist();
        assertThat(metastore.getTable(TABLE.getSchemaName(), "ctas_table")).isEmpty();
        beginTransaction();
        prepareLocalCtas(partitioned, List.of());
        transactionalMetastore.commit();
        assertThat(metastore.getTable(TABLE.getSchemaName(), "ctas_table")).isPresent();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTableLocalCtasFailurePreservesUnrelatedFiles(boolean partitioned)
            throws IOException
    {
        String file = SESSION.getQueryId() + "_new";
        Location staging = prepareLocalCtas(partitioned, List.of(file));
        Location source = partitioned ? staging.appendPath("ds=3") : staging;
        writeFile(source.appendPath(file));
        if (partitioned) {
            addCtasPartition(source, List.of(file));
        }
        Location unrelated = CTAS_LOCATION.appendPath("unrelated");
        writeFile(unrelated);
        Location otherStaging = CTAS_LOCATION.appendPath(".hive-staging/other-operation");
        boundaryFileSystem.createDirectory(otherStaging);
        metastore.failAfterNextUpdate = true;
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(path(unrelated)).exists();
        assertThat(path(otherStaging)).isDirectory();
        assertThat(path(staging)).doesNotExist();
        assertThat(metastore.getTable(TABLE.getSchemaName(), "ctas_table")).isEmpty();
    }

    @Test
    void testTableLocalCtasRollbackBeforeWriteDeclaration()
    {
        HiveLocationService service = new HiveLocationService(_ -> boundaryFileSystem, new HiveConfig().setTableLocalStagingDirectoryEnabled(true));
        service.forNewTableAsSelect(transactionalMetastore, SESSION, TABLE.getSchemaName(), "ctas_table", Optional.empty(), false);
        transactionalMetastore.rollback();
        assertThat(path(CTAS_LOCATION)).doesNotExist();
    }

    @Test
    void testRollbackCleansInsertAndCtasBeforeFragmentsArrive()
            throws IOException
    {
        Location insertStaging = declareLocalWrite(TABLE, TABLE_LOCATION);
        writeFile(insertStaging.appendPath(SESSION.getQueryId() + "_unreported"));
        Location existing = TABLE_LOCATION.appendPath("existing");
        writeFile(existing);
        Location otherStaging = TABLE_LOCATION.appendPath(".hive-staging/other-operation/data");
        writeFile(otherStaging);
        HiveLocationService service = new HiveLocationService(_ -> boundaryFileSystem, new HiveConfig().setTableLocalStagingDirectoryEnabled(true));
        LocationHandle ctas = service.forNewTableAsSelect(transactionalMetastore, SESSION, TABLE.getSchemaName(), "ctas_table", Optional.empty(), false);
        writeFile(service.getQueryWriteInfo(ctas).writePath().appendPath(SESSION.getQueryId() + "_unreported"));

        transactionalMetastore.rollback();

        assertThat(path(insertStaging)).doesNotExist();
        assertThat(path(CTAS_LOCATION)).doesNotExist();
        assertThat(path(existing)).exists();
        assertThat(path(otherStaging)).exists();
    }

    @Test
    void testCtasLocationMasterSwitchAndGlobalMode()
    {
        for (HiveConfig config : List.of(new HiveConfig(), new HiveConfig().setTableLocalStagingDirectoryEnabled(true).setTemporaryStagingDirectoryEnabled(false))) {
            HiveLocationService service = new HiveLocationService(_ -> boundaryFileSystem, config);
            LocationHandle handle = service.forNewTableAsSelect(transactionalMetastore, SESSION, TABLE.getSchemaName(), "ctas_table", Optional.empty(), false);
            assertThat(handle.getWriteMode()).isEqualTo(config.isTemporaryStagingDirectoryEnabled() ? STAGE_AND_MOVE_TO_TARGET_DIRECTORY : DIRECT_TO_TARGET_NEW_DIRECTORY);
            assertThat(handle.getStagingOperationId()).isEmpty();
        }
    }

    @Test
    void testCtasUnsupportedStagingFileSystem()
    {
        metastore.createDatabase(Database.builder()
                .setDatabaseName("memory_database")
                .setLocation(Optional.of("memory:///warehouse"))
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty())
                .build());
        HiveLocationService service = new HiveLocationService(new MemoryFileSystemFactory(), new HiveConfig().setTableLocalStagingDirectoryEnabled(true));
        LocationHandle handle = service.forNewTableAsSelect(transactionalMetastore, SESSION, "memory_database", "table", Optional.empty(), false);
        assertThat(handle.getWriteMode()).isEqualTo(DIRECT_TO_TARGET_NEW_DIRECTORY);
        assertThat(handle.getStagingOperationId()).isEmpty();
    }

    @Test
    void testTableLocalCtasPartitionMetadataFailurePreservesUnrelatedFiles()
            throws IOException
    {
        String file = SESSION.getQueryId() + "_new";
        Location staging = prepareLocalCtas(true, List.of(file));
        writeFile(staging.appendPath("ds=3").appendPath(file));
        addCtasPartition(staging.appendPath("ds=3"), List.of(file));
        writeFile(CTAS_LOCATION.appendPath("unrelated"));
        Location other = CTAS_LOCATION.appendPath(".hive-staging/other-operation/data");
        writeFile(other);
        metastore.failNextPartitionAdd = true;
        assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
        assertThat(path(CTAS_LOCATION.appendPath("ds=3"))).doesNotExist();
        assertThat(path(CTAS_LOCATION.appendPath("unrelated"))).exists();
        assertThat(path(other)).exists();
        assertThat(path(staging)).doesNotExist();
        assertThat(metastore.getTable(TABLE.getSchemaName(), "ctas_table")).isEmpty();
        beginTransaction();
        assertTrinoExceptionThrownBy(() -> prepareLocalCtas(true, List.of()))
                .hasMessageContaining("already exists");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCtasLocationExclusions(boolean transactional)
    {
        HiveLocationService service = new HiveLocationService(_ -> boundaryFileSystem, new HiveConfig().setTableLocalStagingDirectoryEnabled(true));
        LocationHandle external = service.forNewTableAsSelect(transactionalMetastore, SESSION, TABLE.getSchemaName(), "external_ctas", Optional.of(Location.of("local:///external_ctas")), transactional);
        assertThat(external.getWriteMode()).isEqualTo(DIRECT_TO_TARGET_NEW_DIRECTORY);
        if (transactional) {
            LocationHandle managed = service.forNewTableAsSelect(transactionalMetastore, SESSION, TABLE.getSchemaName(), "ctas_table", Optional.empty(), true);
            assertThat(managed.getWriteMode()).isEqualTo(STAGE_AND_MOVE_TO_TARGET_DIRECTORY);
        }
        transactionalMetastore.rollback();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTableLocalNestedPartitionCtas(boolean failMetadata)
            throws IOException
    {
        boundaryFileSystem.enforceBoundaries = true;
        String file = SESSION.getQueryId() + "_new";
        Location staging = prepareLocalCtas(List.of("ds", "hour"), List.of(file));
        Location source = staging.appendPath("ds=3/hour=4");
        Location target = CTAS_LOCATION.appendPath("ds=3/hour=4");
        writeFile(source.appendPath(file));
        Partition partition = Partition.builder(newLocalPartition())
                .setTableName("ctas_table")
                .setValues(List.of("3", "4"))
                .withStorage(storage -> storage.setLocation(target.toString()))
                .build();
        transactionalMetastore.addPartition(SESSION, TABLE.getSchemaName(), "ctas_table", partition, source, Optional.of(List.of(file)), STATISTICS_UPDATE, false);
        if (failMetadata) {
            metastore.failAfterNextUpdate = true;
            assertThatThrownBy(transactionalMetastore::commit).isInstanceOf(TrinoException.class);
            assertThat(path(CTAS_LOCATION)).doesNotExist();
        }
        else {
            transactionalMetastore.commit();
            assertThat(path(target.appendPath(file))).exists();
        }
        assertThat(boundaryFileSystem.directoryRenames).containsExactly(new FileRename(source, target));
    }

    private Location prepareLocalCtas(boolean partitioned, List<String> files)
    {
        return prepareLocalCtas(partitioned ? List.of("ds") : List.of(), files);
    }

    private Location prepareLocalCtas(List<String> partitionColumns, List<String> files)
    {
        boolean partitioned = !partitionColumns.isEmpty();
        HiveLocationService service = new HiveLocationService(_ -> boundaryFileSystem, new HiveConfig().setTableLocalStagingDirectoryEnabled(true));
        LocationHandle handle = service.forNewTableAsSelect(transactionalMetastore, SESSION, TABLE.getSchemaName(), "ctas_table", Optional.empty(), false);
        WriteInfo writeInfo = service.getQueryWriteInfo(handle);
        assertThat(writeInfo.targetPath()).isEqualTo(CTAS_LOCATION);
        assertThat(writeInfo.writePath().toString()).startsWith(CTAS_LOCATION + "/.hive-staging/");
        assertThat(writeInfo.writeMode()).isEqualTo(STAGE_AND_MOVE_FILES_TO_TARGET_DIRECTORY);
        Table table = Table.builder()
                .setDatabaseName(TABLE.getSchemaName())
                .setTableName("ctas_table")
                .setTableType(MANAGED_TABLE.name())
                .setOwner(Optional.of("test"))
                .setDataColumns(dataColumns())
                .setPartitionColumns(partitionColumns.stream().map(name -> new Column(name, HIVE_INT, Optional.empty(), Map.of())).toList())
                .setParameters(Map.of(TRINO_QUERY_ID_NAME, SESSION.getQueryId()))
                .withStorage(storage -> storage.setStorageFormat(ORC.toStorageFormat()).setLocation(CTAS_LOCATION.toString()))
                .build();
        transactionalMetastore.createTable(SESSION, table, NO_PRIVILEGES, Optional.of(writeInfo.writePath()), partitioned ? Optional.empty() : Optional.of(files), false, partitioned ? PartitionStatistics.empty() : STATISTICS_UPDATE, false);
        return writeInfo.writePath();
    }

    private void addCtasPartition(Location staging, List<String> files)
    {
        Partition partition = Partition.builder(newLocalPartition())
                .setTableName("ctas_table")
                .withStorage(storage -> storage.setLocation(CTAS_LOCATION.appendPath("ds=3").toString()))
                .build();
        transactionalMetastore.addPartition(SESSION, TABLE.getSchemaName(), "ctas_table", partition, staging, Optional.of(files), STATISTICS_UPDATE, false);
    }

    private Location declareLocalWrite(SchemaTableName table, Location target)
            throws IOException
    {
        Location staging = target.appendPath(".hive-staging/operation");
        boundaryFileSystem.createDirectory(staging);
        transactionalMetastore.declareTableLocalWrite(SESSION, table, target, staging, false);
        return staging;
    }

    private void finishLocalTableInsert(Location staging, List<String> files)
    {
        transactionalMetastore.finishChangingExistingTable(AcidOperation.INSERT, SESSION, TABLE.getSchemaName(), TABLE.getTableName(), staging, files, STATISTICS_UPDATE, false);
    }

    private static Partition newLocalPartition()
    {
        return Partition.builder(partitionWithStatistics("3", "test_partitioned_table/ds=3").getPartition())
                .setParameters(Map.of(TRINO_QUERY_ID_NAME, SESSION.getQueryId()))
                .build();
    }

    private Path path(Location location)
    {
        return tempDir.resolve(location.path());
    }

    private void writeFile(Location location)
            throws IOException
    {
        Files.createDirectories(path(location).getParent());
        Files.writeString(path(location), "data");
    }

    private record FileRename(Location source, Location target) {}

    private static class BoundaryFileSystem
            extends ForwardingTrinoFileSystem
    {
        private static final List<Location> BOUNDARIES = List.of(
                TABLE_LOCATION,
                CTAS_LOCATION,
                Location.of("local:///data/test_partitioned_table"),
                Location.of("local:///data/" + PARTITION_NAME),
                Location.of("local:///data/" + SECOND_PARTITION_NAME));
        private final List<FileRename> renames = new ArrayList<>();
        private final List<FileRename> directoryRenames = new ArrayList<>();
        private boolean enforceBoundaries;
        private int failRenameNumber;
        private boolean failAfterRename;
        private boolean failVerificationAfterRename;
        private Location verificationFailureTarget;
        private int verificationFailures;
        private boolean failAfterDirectoryRename;
        private boolean createOtherStagingAfterDirectoryRename;
        private boolean failBeforeSourceRemoval;
        private boolean createConflictingDestination;
        private boolean destinationAlreadyExists;

        BoundaryFileSystem(TrinoFileSystem delegate)
        {
            super(delegate);
        }

        @Override
        public void renameDirectory(Location source, Location target)
                throws IOException
        {
            if (enforceBoundaries && (BOUNDARIES.contains(source) || BOUNDARIES.contains(target) || !boundary(source).equals(boundary(target)))) {
                throw new IOException("Directory rename violates target boundary: " + source + " to " + target);
            }
            directoryRenames.add(new FileRename(source, target));
            super.renameDirectory(source, target);
            if (createOtherStagingAfterDirectoryRename) {
                createDirectory(target.appendPath(".hive-staging/other-operation"));
            }
            if (failAfterDirectoryRename) {
                throw new IOException("Injected failure after directory rename");
            }
        }

        @Override
        public void renameFile(Location source, Location target)
                throws IOException
        {
            renames.add(new FileRename(source, target));
            if (enforceBoundaries && !boundary(source).equals(boundary(target))) {
                throw new IOException("File rename crosses target boundary: " + source + " to " + target);
            }
            if (renames.size() == failRenameNumber) {
                throw new IOException("Injected file promotion failure");
            }
            if (createConflictingDestination) {
                try (var output = newOutputFile(target).create()) {
                    output.write("conflicting destination".getBytes(UTF_8));
                }
                throw new IOException("Injected concurrent destination creation");
            }
            if (failBeforeSourceRemoval) {
                try (var input = newInputFile(source).newStream(); var output = newOutputFile(target).create()) {
                    input.transferTo(output);
                }
                if (destinationAlreadyExists) {
                    throw new FileAlreadyExistsException(target.toString());
                }
                throw new IOException("Injected failure with both source and destination present");
            }
            super.renameFile(source, target);
            if (failAfterRename) {
                failAfterRename = false;
                if (failVerificationAfterRename) {
                    verificationFailureTarget = target;
                }
                throw new IOException("Injected failure after rename");
            }
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            TrinoInputFile input = super.newInputFile(location);
            return new TrinoInputFile()
            {
                @Override
                public TrinoInput newInput()
                        throws IOException
                {
                    return input.newInput();
                }

                @Override
                public TrinoInputStream newStream()
                        throws IOException
                {
                    return input.newStream();
                }

                @Override
                public long length()
                        throws IOException
                {
                    return input.length();
                }

                @Override
                public Instant lastModified()
                        throws IOException
                {
                    return input.lastModified();
                }

                @Override
                public boolean exists()
                        throws IOException
                {
                    if (location.equals(verificationFailureTarget)) {
                        verificationFailureTarget = null;
                        verificationFailures++;
                        throw new IOException("Injected destination verification failure");
                    }
                    return input.exists();
                }

                @Override
                public Location location()
                {
                    return location;
                }
            };
        }

        private static Optional<Location> boundary(Location location)
        {
            return BOUNDARIES.stream()
                    .filter(root -> location.equals(root) || location.toString().startsWith(root + "/"))
                    .max(Comparator.comparingInt(root -> root.toString().length()));
        }
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

    private static UpdateStatisticsOperation partitionOperation(String partitionName, StatisticsUpdateMode mode)
    {
        return new UpdateStatisticsOperation(PARTITIONED_TABLE, Optional.of(partitionName), EXISTING_STATISTICS, STATISTICS_UPDATE, mode);
    }

    private static UpdateStatisticsBatch partitionBatch(StatisticsUpdateMode mode)
    {
        return getOnlyElement(UpdateStatisticsBatch.createBatches(
                ImmutableList.of(partitionOperation(PARTITION_NAME, mode), partitionOperation(SECOND_PARTITION_NAME, mode)),
                10));
    }

    private static PartitionWithStatistics partitionWithStatistics(String value, String partitionName)
    {
        Partition partition = Partition.builder()
                .setDatabaseName(PARTITIONED_TABLE.getSchemaName())
                .setTableName(PARTITIONED_TABLE.getTableName())
                .setValues(ImmutableList.of(value))
                .setColumns(dataColumns())
                .setParameters(ImmutableMap.of())
                .withStorage(storage -> storage
                        .setStorageFormat(ORC.toStorageFormat())
                        .setLocation("local:///data/" + partitionName))
                .build();
        return new PartitionWithStatistics(partition, partitionName, EXISTING_STATISTICS);
    }

    private static PartitionUpdateInfo partitionUpdate(String value, String partitionName)
    {
        return new PartitionUpdateInfo(ImmutableList.of(value), Location.of("local:///data/" + partitionName), ImmutableList.of(), STATISTICS_UPDATE, MERGE_INCREMENTAL);
    }

    private PartitionStatistics storedStatistics()
    {
        Table table = metastore.getTable(TABLE.getSchemaName(), TABLE.getTableName()).orElseThrow();
        return new PartitionStatistics(
                getHiveBasicStatistics(table.getParameters()),
                metastore.getTableColumnStatistics(TABLE.getSchemaName(), TABLE.getTableName(), ImmutableSet.of("a", "b")));
    }

    private PartitionStatistics storedPartitionStatistics(String partitionName)
    {
        Table table = metastore.getTable(PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName()).orElseThrow();
        Partition partition = metastore.getPartition(table, toPartitionValues(partitionName)).orElseThrow();
        return new PartitionStatistics(
                getHiveBasicStatistics(partition.getParameters()),
                metastore.getPartitionColumnStatistics(PARTITIONED_TABLE.getSchemaName(), PARTITIONED_TABLE.getTableName(), ImmutableSet.of(partitionName), ImmutableSet.of("a", "b")).get(partitionName));
    }

    // applies the update, then reports it as failed, like a metastore response lost after the write
    private static class PartiallyFailingMetastore
            extends FileHiveMetastore
    {
        private boolean failAfterNextUpdate;
        private boolean failNextPartitionAdd;
        private boolean failNextTableCreate;

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
        public synchronized void createTable(Table table, PrincipalPrivileges privileges)
        {
            if (failNextTableCreate) {
                failNextTableCreate = false;
                throw new TrinoException(HIVE_METASTORE_ERROR, "table create failed");
            }
            super.createTable(table, privileges);
        }

        @Override
        public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
        {
            throw new AssertionError("Cleanup must not enumerate metastore partitions");
        }

        @Override
        public synchronized void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
        {
            if (failNextPartitionAdd) {
                failNextPartitionAdd = false;
                throw new TrinoException(HIVE_METASTORE_ERROR, "partition add failed");
            }
            super.addPartitions(databaseName, tableName, partitions);
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

        @Override
        public synchronized void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
        {
            super.updatePartitionStatistics(table, mode, partitionUpdates);
            if (failAfterNextUpdate) {
                failAfterNextUpdate = false;
                throw new TrinoException(HIVE_METASTORE_ERROR, "statistics update response lost");
            }
        }
    }
}
