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
package io.trino.plugin.deltalake.clustering;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.deltalake.DefaultDeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeSessionProperties;
import io.trino.plugin.deltalake.metastore.NoOpVendedCredentialsProvider;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.reader.FileSystemTransactionLogReader;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.deltalake.DeltaLakeConfig.DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE;
import static io.trino.plugin.deltalake.clustering.ClusteringMetadataUtil.extractCommitInfo;
import static io.trino.plugin.deltalake.clustering.ClusteringMetadataUtil.getClusteredColumnList;
import static io.trino.plugin.deltalake.clustering.ClusteringMetadataUtil.getCurrentVersion;
import static io.trino.plugin.deltalake.clustering.ClusteringMetadataUtil.getOldToNewRenamedColumns;
import static io.trino.plugin.deltalake.clustering.ClusteringMetadataUtil.getOperation;
import static io.trino.plugin.deltalake.clustering.ClusteringMetadataUtil.getRestoreVersion;
import static io.trino.plugin.deltalake.clustering.ClusteringMetadataUtil.recordRenamedColumns;
import static io.trino.plugin.deltalake.clustering.ClusteringMetadataUtil.shouldStopLookup;
import static io.trino.plugin.deltalake.clustering.Operation.ADD_COLUMNS;
import static io.trino.plugin.deltalake.clustering.Operation.ADD_CONSTRAINT;
import static io.trino.plugin.deltalake.clustering.Operation.ADD_DELETION_VECTOR_TOMBSTONES;
import static io.trino.plugin.deltalake.clustering.Operation.CHANGE_COLUMN;
import static io.trino.plugin.deltalake.clustering.Operation.CHANGE_COLUMNS;
import static io.trino.plugin.deltalake.clustering.Operation.CLONE;
import static io.trino.plugin.deltalake.clustering.Operation.CLUSTER_BY;
import static io.trino.plugin.deltalake.clustering.Operation.COMPUTE_STATS;
import static io.trino.plugin.deltalake.clustering.Operation.CONVERT;
import static io.trino.plugin.deltalake.clustering.Operation.CREATE_TABLE_KEYWORD;
import static io.trino.plugin.deltalake.clustering.Operation.DELETE;
import static io.trino.plugin.deltalake.clustering.Operation.DOMAIN_METADATA_CLEANUP;
import static io.trino.plugin.deltalake.clustering.Operation.DROP_COLUMNS;
import static io.trino.plugin.deltalake.clustering.Operation.DROP_CONSTRAINT;
import static io.trino.plugin.deltalake.clustering.Operation.DROP_TABLE_FEATURE;
import static io.trino.plugin.deltalake.clustering.Operation.EMPTY_COMMIT;
import static io.trino.plugin.deltalake.clustering.Operation.MANUAL_UPDATE;
import static io.trino.plugin.deltalake.clustering.Operation.MERGE;
import static io.trino.plugin.deltalake.clustering.Operation.OPTIMIZE;
import static io.trino.plugin.deltalake.clustering.Operation.REMOVE_COLUMN_MAPPING;
import static io.trino.plugin.deltalake.clustering.Operation.RENAME_COLUMN;
import static io.trino.plugin.deltalake.clustering.Operation.REORG;
import static io.trino.plugin.deltalake.clustering.Operation.REORG_TABLE_UPGRADE_UNIFORM;
import static io.trino.plugin.deltalake.clustering.Operation.REPLACE_COLUMNS;
import static io.trino.plugin.deltalake.clustering.Operation.REPLACE_TABLE_KEYWORD;
import static io.trino.plugin.deltalake.clustering.Operation.RESTORE;
import static io.trino.plugin.deltalake.clustering.Operation.ROW_TRACKING_BACKFILL;
import static io.trino.plugin.deltalake.clustering.Operation.ROW_TRACKING_UNBACKFILL;
import static io.trino.plugin.deltalake.clustering.Operation.SET_TABLE_PROPERTIES;
import static io.trino.plugin.deltalake.clustering.Operation.STREAMING_UPDATE;
import static io.trino.plugin.deltalake.clustering.Operation.TEST_OPERATION;
import static io.trino.plugin.deltalake.clustering.Operation.TRUNCATE;
import static io.trino.plugin.deltalake.clustering.Operation.UNKNOW_OPERATION;
import static io.trino.plugin.deltalake.clustering.Operation.UNSET_TABLE_PROPERTIES;
import static io.trino.plugin.deltalake.clustering.Operation.UPDATE;
import static io.trino.plugin.deltalake.clustering.Operation.UPDATE_COLUMN_METADATA;
import static io.trino.plugin.deltalake.clustering.Operation.UPDATE_SCHEMA;
import static io.trino.plugin.deltalake.clustering.Operation.UPGRADE_PROTOCOL;
import static io.trino.plugin.deltalake.clustering.Operation.VACUUM_END;
import static io.trino.plugin.deltalake.clustering.Operation.VACUUM_START;
import static io.trino.plugin.deltalake.clustering.Operation.WRITE;
import static io.trino.plugin.deltalake.transactionlog.TableSnapshot.load;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestClusteringMetadataUtil
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new DeltaLakeSessionProperties(new DeltaLakeConfig(), new ParquetReaderConfig(), new ParquetWriterConfig()).getSessionProperties())
            .setPropertyValues(ImmutableMap.of("enable_clustering_info", true))
            .build();

    private static final VendedCredentialsHandle CREDENTIALS_HANDLE = VendedCredentialsHandle.empty("test");
    private static final DefaultDeltaLakeFileSystemFactory DEFAULT_DELTA_LAKE_FILE_SYSTEM_FACTORY = new DefaultDeltaLakeFileSystemFactory(
            new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS),
            new NoOpVendedCredentialsProvider());
    private static final int DOMAIN_COMPACTION_THRESHOLD = 32;
    private static final TrinoFileSystem FILE_SYSTEM = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);

    private static class TableSnapShotBuilderForTestBuilder
    {
        private String tableLocation;

        public TableSnapShotBuilderForTestBuilder withTableLocation(String tableLocation)
        {
            this.tableLocation = tableLocation;
            return this;
        }

        public TableSnapshot build()
                throws IOException
        {
            return load(
                    SESSION,
                    new FileSystemTransactionLogReader(tableLocation, CREDENTIALS_HANDLE, DEFAULT_DELTA_LAKE_FILE_SYSTEM_FACTORY),
                    new SchemaTableName("schema", "test"),
                    Optional.empty(),
                    tableLocation,
                    ParquetReaderOptions.defaultOptions(),
                    true,
                    DOMAIN_COMPACTION_THRESHOLD,
                    DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE,
                    Optional.empty());
        }
    }

    private static class CommitInfoEntryForTestBuilder
    {
        private String operation;
        private Map<String, String> operationParameters;

        public CommitInfoEntryForTestBuilder withOperation(String operation)
        {
            this.operation = operation;
            return this;
        }

        public CommitInfoEntryForTestBuilder withOperationParameters(Map<String, String> operationParameters)
        {
            this.operationParameters = operationParameters;
            return this;
        }

        public CommitInfoEntry build()
        {
            requireNonNull(operation, "operation is null");
            requireNonNull(operationParameters, "operationParameters is null");
            long timestamp = Instant.now().toEpochMilli();
            return new CommitInfoEntry(
                    0,
                    OptionalLong.of(timestamp),
                    timestamp,
                    "user1",
                    "user1",
                    operation,
                    ImmutableMap.copyOf(operationParameters),
                    null,
                    null,
                    "cluster_1",
                    0,
                    "SnapshotIsolation",
                    Optional.of(true),
                    ImmutableMap.of());
        }
    }

    @Test
    void testGetLatestClusteredColumnsWithAutoOptimize()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_auto_optimize";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).isEmpty();
    }

    @Test
    void testGetLatestClusteredColumnsWithCloneTable()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_clone_table";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).isEmpty();
    }

    @Test
    void testGetLatestClusteredColumnsWithCreateOrReplaceTable()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_create_or_replace_table";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).containsExactly("col_1", "col_2");
    }

    @Test
    void testGetLatestClusteredColumnsWithCreateTable()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_create_table";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).containsExactly("col_1", "col_2");
    }

    @Test
    void testGetLatestClusteredColumnsWithCreateTableAsSelect()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_create_table_as_select";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).containsExactly("col_1", "col_2");
    }

    @Test
    void testGetLatestClusteredColumnsWithManuallyOptimize()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_manually_optimize";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).containsExactly("manually_optimize_col_1", "manually_optimize_col_2");
    }

    @Test
    void testGetLatestClusteredColumnsWithMerge()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_merge";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).containsExactly("merge_col_1", "merge_col_2");
    }

    @Test
    void testGetLatestClusteredColumnsWithClusteredOperation()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_cluster_by";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).containsExactly("new_col_1", "new_col_2");
    }

    @Test
    void testGetLatestClusteredColumnsWithRenameColumn()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_renamed_clustered_column";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).containsExactly("col_rename_latest_time_1", "col_before_rename_2");
    }

    @Test
    void testGetLatestClusteredColumnsWithRestoreToTimestamp()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_restore_to_timestamp";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).containsExactly("commit_1_col_1", "commit_1_col_2");
    }

    @Test
    void testGetLatestClusteredColumnsWithRestoreToVersion()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_restore_to_version";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).containsExactly("commit_2_col_1", "commit_2_col_2");
    }

    @Test
    void testGetLatestClusteredColumnsWithRestoreToVersionTimestampRecursive()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_restore_version_timestamp_recursive";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).containsExactly("commit_0_col_1", "commit_0_col_2");
    }

    @Test
    void testGetLatestClusteredColumnsWithShallowCloneTable()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_shallow_clone_table";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).isEmpty();
    }

    @Test
    void testGetLatestClusteredColumnsWithWrite()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_write";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).containsExactly("commit_0_col_1", "commit_0_col_2");
    }

    @Test
    void testGetLatestClusteredColumnsOtherNoClusterOperations()
            throws IOException
    {
        String tableLocation = "databricks172/liquid_clustering_with_operations/liquid_clustering_with_other_no_cluster_operations";
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation(tableLocation))
                .build();

        Optional<List<String>> result = ClusteringMetadataUtil.getLatestClusteredColumns(FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get()).isEmpty();
    }

    @Test
    void testGetCurrentVersionWhenNotRestore()
            throws IOException
    {
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation("databricks172/liquid_clustering_with_operations/liquid_clustering_with_write"))
                .build();
        long version = getCurrentVersion(FILE_SYSTEM, tableSnapshot, 0);

        assertThat(version).isEqualTo(0);
        assertThat(tableSnapshot.getVersion()).isEqualTo(0);
    }

    @Test
    void testGetCurrentVersionWhenRestoreToVersion()
            throws IOException
    {
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation("databricks172/liquid_clustering_with_operations/liquid_clustering_with_restore_to_version"))
                .build();
        long version = getCurrentVersion(FILE_SYSTEM, tableSnapshot, 4);

        assertThat(version).isEqualTo(2);
        assertThat(tableSnapshot.getVersion()).isNotEqualTo(2);
    }

    @Test
    void testGetCurrentVersionWhenRestoreToTimestamp()
            throws IOException
    {
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation("databricks172/liquid_clustering_with_operations/liquid_clustering_with_restore_to_timestamp"))
                .build();
        long version = getCurrentVersion(FILE_SYSTEM, tableSnapshot, 4);

        assertThat(version).isEqualTo(1);
        assertThat(tableSnapshot.getVersion()).isNotEqualTo(1);
    }

    @Test
    void testGetCurrentVersionException()
            throws IOException
    {
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation("databricks172/liquid_clustering_with_operations/liquid_clustering_with_no_commit"))
                .build();

        assertThatThrownBy(() -> getCurrentVersion(FILE_SYSTEM, tableSnapshot, 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No commit info found for table at version 0");
    }

    @Test
    void testGetRestoreVersionWithVersion()
            throws IOException
    {
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation("databricks172/liquid_clustering_with_operations/liquid_clustering_with_restore_to_version"))
                .build();
        CommitInfoEntry commitInfoEntry = new CommitInfoEntryForTestBuilder()
                .withOperation("RESTORE")
                .withOperationParameters(ImmutableMap.of("version", "0"))
                .build();

        long version = getRestoreVersion(commitInfoEntry, FILE_SYSTEM, tableSnapshot);

        assertThat(version).isEqualTo(0);
        assertThat(tableSnapshot.getVersion()).isNotEqualTo(0);
    }

    @Test
    void testGetRestoreVersionWithTimestamp()
            throws IOException
    {
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation("databricks172/liquid_clustering_with_operations/liquid_clustering_with_restore_to_timestamp"))
                .build();
        CommitInfoEntry commitInfoEntry = new CommitInfoEntryForTestBuilder()
                .withOperation("RESTORE")
                .withOperationParameters(ImmutableMap.of("timestamp", "2025-10-17 16:45:00.0"))
                .build();

        long version = getRestoreVersion(commitInfoEntry, FILE_SYSTEM, tableSnapshot);

        assertThat(version).isEqualTo(1);
        assertThat(tableSnapshot.getVersion()).isNotEqualTo(1);
    }

    @Test
    void testGetRestoreVersionNoRestoreOperation()
            throws IOException
    {
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation("databricks172/liquid_clustering_with_operations/liquid_clustering_with_restore_to_timestamp"))
                .build();
        CommitInfoEntry commitInfoEntry = new CommitInfoEntryForTestBuilder()
                .withOperation("WRITE")
                .withOperationParameters(ImmutableMap.of())
                .build();

        assertThatThrownBy(() -> getRestoreVersion(commitInfoEntry, FILE_SYSTEM, tableSnapshot))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not of RESTORE operation");
    }

    @Test
    void testGetRestoreVersionNoVersionAndTimestamp()
            throws IOException
    {
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation("databricks172/liquid_clustering_with_operations/liquid_clustering_with_restore_to_timestamp"))
                .build();
        CommitInfoEntry commitInfoEntry = new CommitInfoEntryForTestBuilder()
                .withOperation("RESTORE")
                .withOperationParameters(ImmutableMap.of())
                .build();

        assertThatThrownBy(() -> getRestoreVersion(commitInfoEntry, FILE_SYSTEM, tableSnapshot))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Both restored version and timestamp are null or empty, should never happen");
    }

    @Test
    void testExtractCommitInfoWithCommitInfo()
            throws IOException
    {
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation("databricks172/liquid_clustering_with_operations/liquid_clustering_with_write"))
                .build();

        Optional<CommitInfoEntry> result = extractCommitInfo(0L, FILE_SYSTEM, tableSnapshot);

        assertThat(result).isPresent();
        assertThat(result.get().operation()).isEqualTo("WRITE");
    }

    @Test
    void testExtractCommitInfoWithoutCommitInfo()
            throws IOException
    {
        TableSnapshot tableSnapshot = new TableSnapShotBuilderForTestBuilder()
                .withTableLocation(getResourceLocation("databricks172/liquid_clustering_with_operations/liquid_clustering_with_no_commit"))
                .build();

        assertThatThrownBy(() -> extractCommitInfo(0L, FILE_SYSTEM, tableSnapshot))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No commit info found for table at version");
    }

    @Test
    void testExtractClusteredColumnsValidClusteredValue()
    {
        CommitInfoEntry commitInfoEntry = new CommitInfoEntryForTestBuilder()
                .withOperation("WRITE")
                .withOperationParameters(ImmutableMap.of("clusterBy", "[\"col1\",\"col2\"]"))
                .build();

        Optional<CommitInfoEntry> commitInfo = Optional.of(commitInfoEntry);
        List<String> result = ClusteringMetadataUtil.extractClusteredColumns(commitInfo);

        assertThat(result).containsExactly("col1", "col2");
    }

    @Test
    void testExtractClusteredColumnsEmptyClusteredValue()
    {
        CommitInfoEntry commitInfoEntry = new CommitInfoEntryForTestBuilder()
                .withOperation("WRITE")
                .withOperationParameters(ImmutableMap.of("clusterBy", "[]"))
                .build();

        Optional<CommitInfoEntry> commitInfo = Optional.of(commitInfoEntry);
        List<String> result = ClusteringMetadataUtil.extractClusteredColumns(commitInfo);

        assertThat(result).isEmpty();
    }

    @Test
    void testExtractClusteredColumnsInValidClusteredValue()
    {
        CommitInfoEntry commitInfoEntry = new CommitInfoEntryForTestBuilder()
                .withOperation("WRITE")
                .withOperationParameters(ImmutableMap.of("clusterBy", "[col1,col2]"))
                .build();

        Optional<CommitInfoEntry> commitInfo = Optional.of(commitInfoEntry);
        List<String> result = ClusteringMetadataUtil.extractClusteredColumns(commitInfo);

        assertThat(result).isEmpty();
    }

    @Test
    void testExtractClusteredColumnsWithNoClusterInfo()
    {
        CommitInfoEntry commitInfoEntry = new CommitInfoEntryForTestBuilder()
                .withOperation("TRUNCATE")
                .withOperationParameters(ImmutableMap.of())
                .build();

        Optional<CommitInfoEntry> commitInfo = Optional.of(commitInfoEntry);
        List<String> result = ClusteringMetadataUtil.extractClusteredColumns(commitInfo);

        assertThat(result).isEmpty();
    }

    @Test
    void testExtractClusteredColumnsWithUnknowOperation()
    {
        CommitInfoEntry commitInfoEntry = new CommitInfoEntryForTestBuilder()
                .withOperation("WHATEVER")
                .withOperationParameters(ImmutableMap.of())
                .build();

        Optional<CommitInfoEntry> commitInfo = Optional.of(commitInfoEntry);
        List<String> result = ClusteringMetadataUtil.extractClusteredColumns(commitInfo);

        assertThat(result).isEmpty();
    }

    @Test
    void testExtractClusteredColumnsWithRenameOperation()
    {
        CommitInfoEntry commitInfoEntry = new CommitInfoEntryForTestBuilder()
                .withOperation("RENAME COLUMN")
                .withOperationParameters(ImmutableMap.of("oldColumnPath", "old_col", "newColumnPath", "new_col"))
                .build();

        Optional<CommitInfoEntry> commitInfo = Optional.of(commitInfoEntry);
        List<String> result = ClusteringMetadataUtil.extractClusteredColumns(commitInfo);

        assertThat(getOldToNewRenamedColumns().get()).isNotEmpty();
        assertThat(getOldToNewRenamedColumns().get().get("old_col")).isEqualTo("new_col");

        getOldToNewRenamedColumns().remove();
        assertThat(result).isEmpty();
    }

    @Test
    void testShouldStopLookup()
    {
        assertThat(shouldStopLookup(OPTIMIZE, ImmutableList.of("col1"))).isTrue();
        assertThat(shouldStopLookup(OPTIMIZE, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(WRITE, ImmutableList.of("col1"))).isTrue();
        assertThat(shouldStopLookup(WRITE, ImmutableList.of())).isTrue();
        assertThat(shouldStopLookup(MERGE, ImmutableList.of("col1"))).isTrue();
        assertThat(shouldStopLookup(MERGE, ImmutableList.of())).isTrue();
        assertThat(shouldStopLookup(CREATE_TABLE_KEYWORD, ImmutableList.of("col1"))).isTrue();
        assertThat(shouldStopLookup(CREATE_TABLE_KEYWORD, ImmutableList.of())).isTrue();
        assertThat(shouldStopLookup(REPLACE_TABLE_KEYWORD, ImmutableList.of("col1"))).isTrue();
        assertThat(shouldStopLookup(REPLACE_TABLE_KEYWORD, ImmutableList.of())).isTrue();
        assertThat(shouldStopLookup(CLUSTER_BY, ImmutableList.of("col1"))).isTrue();
        assertThat(shouldStopLookup(CLUSTER_BY, ImmutableList.of())).isTrue();
        assertThat(shouldStopLookup(RENAME_COLUMN, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(RENAME_COLUMN, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(ADD_COLUMNS, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(ADD_CONSTRAINT, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(ADD_DELETION_VECTOR_TOMBSTONES, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(CHANGE_COLUMN, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(CHANGE_COLUMNS, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(CLONE, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(COMPUTE_STATS, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(CONVERT, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(DELETE, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(DOMAIN_METADATA_CLEANUP, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(DROP_COLUMNS, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(DROP_CONSTRAINT, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(DROP_TABLE_FEATURE, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(EMPTY_COMMIT, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(MANUAL_UPDATE, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(REORG, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(REORG_TABLE_UPGRADE_UNIFORM, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(REMOVE_COLUMN_MAPPING, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(REPLACE_COLUMNS, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(RESTORE, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(ROW_TRACKING_BACKFILL, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(ROW_TRACKING_UNBACKFILL, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(SET_TABLE_PROPERTIES, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(STREAMING_UPDATE, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(TRUNCATE, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(UNSET_TABLE_PROPERTIES, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(UPDATE, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(UPDATE_COLUMN_METADATA, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(UPDATE_SCHEMA, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(UPGRADE_PROTOCOL, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(VACUUM_END, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(VACUUM_START, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(TEST_OPERATION, ImmutableList.of())).isFalse();
        assertThat(shouldStopLookup(UNKNOW_OPERATION, ImmutableList.of())).isFalse();
    }

    @Test
    void testGetOperation()
    {
        for (Operation op : Operation.values()) {
            Operation resolved = getOperation(op.getOperationName());
            assertThat(op).isEqualTo(resolved);
        }
        assertThat(UNKNOW_OPERATION).isEqualTo(getOperation("NON_EXISTENT_OPERATION"));
        assertThatThrownBy(() -> getOperation(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Operation parameter is empty");
    }

    @Test
    void testRecordRenamedColumnsNormalPut()
    {
        CommitInfoEntry commitInfoEntry = new CommitInfoEntryForTestBuilder()
                .withOperation("RENAME COLUMN")
                .withOperationParameters(ImmutableMap.of("oldColumnPath", "old_col", "newColumnPath", "new_col"))
                .build();

        recordRenamedColumns(commitInfoEntry);

        assertThat(getOldToNewRenamedColumns().get()).isNotEmpty();
        assertThat(getOldToNewRenamedColumns().get().size()).isEqualTo(1);
        assertThat(getOldToNewRenamedColumns().get().get("old_col")).isEqualTo("new_col");

        getOldToNewRenamedColumns().remove();
    }

    @Test
    void testRecordRenamedColumnsPutExisted()
    {
        getOldToNewRenamedColumns().get().put("new_col", "new_col_2");
        CommitInfoEntry commitInfoEntry = new CommitInfoEntryForTestBuilder()
                .withOperation("RENAME COLUMN")
                .withOperationParameters(ImmutableMap.of("oldColumnPath", "old_col", "newColumnPath", "new_col"))
                .build();

        recordRenamedColumns(commitInfoEntry);

        assertThat(getOldToNewRenamedColumns().get()).isNotEmpty();
        assertThat(getOldToNewRenamedColumns().get().size()).isEqualTo(1);
        assertThat(getOldToNewRenamedColumns().get().get("old_col")).isEqualTo("new_col_2");

        getOldToNewRenamedColumns().remove();
    }

    @Test
    void testRecordRenamedColumnsMissingOldColumn()
    {
        CommitInfoEntry commitInfoEntry = new CommitInfoEntryForTestBuilder()
                .withOperation("RENAME COLUMN")
                .withOperationParameters(ImmutableMap.of("oldColumnPath", "", "newColumnPath", "new_col"))
                .build();

        assertThatThrownBy(() -> recordRenamedColumns(commitInfoEntry))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("old or renamed columns are null or empty, should never happen");

        getOldToNewRenamedColumns().remove();
    }

    @Test
    void testRecordRenamedColumnsMissingNewColumn()
    {
        CommitInfoEntry commitInfoEntry = new CommitInfoEntryForTestBuilder()
                .withOperation("RENAME COLUMN")
                .withOperationParameters(ImmutableMap.of("oldColumnPath", "old_col", "newColumnPath", ""))
                .build();

        assertThatThrownBy(() -> recordRenamedColumns(commitInfoEntry))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("old or renamed columns are null or empty, should never happen");

        getOldToNewRenamedColumns().remove();
    }

    @Test
    void testGetClusteredColumnListWithClusterBy()
    {
        String clusteredKey = "clusterBy";
        String clusteredValue = "[\"col1\", \"col2\"]";

        List<String> result = getClusteredColumnList(clusteredKey, clusteredValue);

        assertThat(result).containsExactly("col1", "col2");
    }

    @Test
    void testGetClusteredColumnListWithNewClusteringColumns()
    {
        String clusteredKey = "newClusteringColumns";
        String clusteredValue = "col1,col2";

        List<String> result = getClusteredColumnList(clusteredKey, clusteredValue);

        assertThat(result).isNotEmpty();
        assertThat(result).containsExactly("col1", "col2");
    }

    @Test
    void testGetClusteredColumnListWithInvalidJson()
    {
        String clusteredKey = "clusterBy";
        String clusteredValue = "col1,col2";

        List<String> result = getClusteredColumnList(clusteredKey, clusteredValue);

        assertThat(result).isEmpty();
    }

    @Test
    void testGetClusteredColumnListWithUnknown()
    {
        String clusteredKey = "unknown";
        String clusteredValue = "col1,col2";

        List<String> result = getClusteredColumnList(clusteredKey, clusteredValue);

        assertThat(result).isEmpty();
    }

    private String getResourceLocation(String resourcePath)
    {
        return requireNonNull(getClass().getClassLoader().getResource(resourcePath)).getPath();
    }
}
