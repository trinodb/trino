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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.DataSize;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TypeManager;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.testing.assertions.Assert.assertEquals;

public class TestDeltaLakeSplitManager
{
    private static final String TABLE_PATH = "/path/to/a/table";
    private static final String FILE_PATH = "directory/file";
    private static final String FULL_PATH = TABLE_PATH + "/" + FILE_PATH;
    private static final MetadataEntry metadataEntry = new MetadataEntry(
            "id",
            "name",
            "description",
            new MetadataEntry.Format("provider", ImmutableMap.of()),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"val\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}",
            ImmutableList.of(),
            ImmutableMap.of(),
            0);
    private static final DeltaLakeTableHandle tableHandle = new DeltaLakeTableHandle(
            "schema",
            "table",
            "location",
            Optional.of(metadataEntry),
            TupleDomain.all(),
            TupleDomain.all(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            0,
            false);

    @Test
    public void testInitialSplits()
            throws ExecutionException, InterruptedException
    {
        long fileSize = 20_000;
        List<AddFileEntry> addFileEntries = ImmutableList.of(addFileEntryOfSize(fileSize));
        DeltaLakeConfig deltalakeConfig = new DeltaLakeConfig()
                .setMaxInitialSplits(1000)
                .setMaxInitialSplitSize(DataSize.ofBytes(5_000));

        DeltaLakeSplitManager splitManager = setupSplitManager(addFileEntries, deltalakeConfig);
        List<DeltaLakeSplit> splits = getSplits(splitManager, deltalakeConfig);

        List<DeltaLakeSplit> expected = ImmutableList.of(
                makeSplit(0, 5_000, fileSize),
                makeSplit(5_000, 5_000, fileSize),
                makeSplit(10_000, 5_000, fileSize),
                makeSplit(15_000, 5_000, fileSize));

        assertEquals(splits, expected);
    }

    @Test
    public void testNonInitialSplits()
            throws ExecutionException, InterruptedException
    {
        long fileSize = 50_000;
        List<AddFileEntry> addFileEntries = ImmutableList.of(addFileEntryOfSize(fileSize));
        DeltaLakeConfig deltalakeConfig = new DeltaLakeConfig()
                .setMaxInitialSplits(5)
                .setMaxInitialSplitSize(DataSize.ofBytes(5_000))
                .setMaxSplitSize(DataSize.ofBytes(20_000));

        DeltaLakeSplitManager splitManager = setupSplitManager(addFileEntries, deltalakeConfig);
        List<DeltaLakeSplit> splits = getSplits(splitManager, deltalakeConfig);

        List<DeltaLakeSplit> expected = ImmutableList.of(
                makeSplit(0, 5_000, fileSize),
                makeSplit(5_000, 5_000, fileSize),
                makeSplit(10_000, 5_000, fileSize),
                makeSplit(15_000, 5_000, fileSize),
                makeSplit(20_000, 5_000, fileSize),
                makeSplit(25_000, 20_000, fileSize),
                makeSplit(45_000, 5_000, fileSize));

        assertEquals(splits, expected);
    }

    @Test
    public void testSplitsFromMultipleFiles()
            throws ExecutionException, InterruptedException
    {
        long firstFileSize = 1_000;
        long secondFileSize = 20_000;
        List<AddFileEntry> addFileEntries = ImmutableList.of(addFileEntryOfSize(firstFileSize), addFileEntryOfSize(secondFileSize));
        DeltaLakeConfig deltalakeConfig = new DeltaLakeConfig()
                .setMaxInitialSplits(3)
                .setMaxInitialSplitSize(DataSize.ofBytes(2_000))
                .setMaxSplitSize(DataSize.ofBytes(10_000));

        DeltaLakeSplitManager splitManager = setupSplitManager(addFileEntries, deltalakeConfig);

        List<DeltaLakeSplit> splits = getSplits(splitManager, deltalakeConfig);
        List<DeltaLakeSplit> expected = ImmutableList.of(
                makeSplit(0, 1_000, firstFileSize),
                makeSplit(0, 2_000, secondFileSize),
                makeSplit(2_000, 2_000, secondFileSize),
                makeSplit(4_000, 10_000, secondFileSize),
                makeSplit(14_000, 6_000, secondFileSize));
        assertEquals(splits, expected);
    }

    private DeltaLakeSplitManager setupSplitManager(List<AddFileEntry> addFileEntries, DeltaLakeConfig deltaLakeConfig)
    {
        TestingConnectorContext context = new TestingConnectorContext();
        TypeManager typeManager = context.getTypeManager();

        MockDeltaLakeMetastore metastore = new MockDeltaLakeMetastore();
        metastore.setValidDataFiles(addFileEntries);
        return new DeltaLakeSplitManager(
                typeManager,
                (session, transaction) -> metastore,
                MoreExecutors.newDirectExecutorService(),
                deltaLakeConfig);
    }

    private AddFileEntry addFileEntryOfSize(long fileSize)
    {
        return new AddFileEntry(FILE_PATH, ImmutableMap.of(), fileSize, 0, false, Optional.empty(), Optional.empty(), ImmutableMap.of());
    }

    private DeltaLakeSplit makeSplit(long start, long splitSize, long fileSize)
    {
        return new DeltaLakeSplit(FULL_PATH, start, splitSize, fileSize, 0, ImmutableList.of(), TupleDomain.all(), ImmutableMap.of());
    }

    private List<DeltaLakeSplit> getSplits(DeltaLakeSplitManager splitManager, DeltaLakeConfig deltaLakeConfig)
            throws ExecutionException, InterruptedException
    {
        ConnectorSplitSource splitSource = splitManager.getSplits(
                // ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle handle, SplitSchedulingStrategy splitSchedulingStrategy
                new HiveTransactionHandle(false),
                testingConnectorSessionWithConfig(deltaLakeConfig),
                tableHandle,
                ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING,
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue());
        ImmutableList.Builder<DeltaLakeSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            List<ConnectorSplit> nextBatch = splitSource.getNextBatch(NOT_PARTITIONED, 10).get().getSplits();
            splits.addAll(
                    nextBatch.stream()
                            .map(split -> (DeltaLakeSplit) split)
                            .collect(Collectors.toList()));
        }
        return splits.build();
    }

    private ConnectorSession testingConnectorSessionWithConfig(DeltaLakeConfig deltaLakeConfig)
    {
        DeltaLakeSessionProperties sessionProperties = new DeltaLakeSessionProperties(deltaLakeConfig, new ParquetReaderConfig(), new ParquetWriterConfig());
        return TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();
    }

    private static class MockDeltaLakeMetastore
            implements DeltaLakeMetastore
    {
        private List<AddFileEntry> validDataFiles;

        public void setValidDataFiles(List<AddFileEntry> validDataFiles)
        {
            this.validDataFiles = ImmutableList.copyOf(validDataFiles);
        }

        @Override
        public List<String> getAllDatabases()
        {
            throw new UnsupportedOperationException("Unimplemented");
        }

        @Override
        public Optional<Database> getDatabase(String databaseName)
        {
            throw new UnsupportedOperationException("Unimplemented");
        }

        @Override
        public List<String> getAllTables(String databaseName)
        {
            throw new UnsupportedOperationException("Unimplemented");
        }

        @Override
        public Optional<Table> getTable(String databaseName, String tableName)
        {
            throw new UnsupportedOperationException("Unimplemented");
        }

        @Override
        public void createDatabase(Database database)
        {
            throw new UnsupportedOperationException("Unimplemented");
        }

        @Override
        public void dropDatabase(String databaseName, boolean deleteData)
        {
            throw new UnsupportedOperationException("Unimplemented");
        }

        @Override
        public void createTable(ConnectorSession session, Table table, PrincipalPrivileges principalPrivileges)
        {
            throw new UnsupportedOperationException("Unimplemented");
        }

        @Override
        public void dropTable(ConnectorSession session, String databaseName, String tableName)
        {
            throw new UnsupportedOperationException("Unimplemented");
        }

        @Override
        public Optional<MetadataEntry> getMetadata(TableSnapshot tableSnapshot, ConnectorSession session)
        {
            throw new UnsupportedOperationException("Unimplemented");
        }

        @Override
        public ProtocolEntry getProtocol(ConnectorSession session, TableSnapshot tableSnapshot)
        {
            throw new UnsupportedOperationException("Unimplemented");
        }

        @Override
        public String getTableLocation(SchemaTableName table, ConnectorSession session)
        {
            return TABLE_PATH;
        }

        @Override
        public TableSnapshot getSnapshot(SchemaTableName table, ConnectorSession session)
        {
            throw new UnsupportedOperationException("Unimplemented");
        }

        @Override
        public List<AddFileEntry> getValidDataFiles(SchemaTableName table, ConnectorSession session)
        {
            return validDataFiles;
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, DeltaLakeTableHandle tableHandle, Constraint constraint)
        {
            throw new UnsupportedOperationException("Unimplemented");
        }

        @Override
        public HiveMetastore getHiveMetastore()
        {
            throw new UnsupportedOperationException("Unimplemented");
        }
    }
}
