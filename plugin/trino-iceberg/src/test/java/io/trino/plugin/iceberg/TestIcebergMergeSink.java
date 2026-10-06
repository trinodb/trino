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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.delete.DeletionVector;
import io.trino.spi.NodeVersion;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.MemoryContext;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.PartitionSpec;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergMergeSink
{
    @Test
    void testDeletionsGroupedByFileAcrossRunsAndPages()
    {
        IcebergMergeSink mergeSink = createMergeSink(new TestingInsertPageSink(0), _ -> {});

        mergeSink.storeMergedRows(deletionsPage(ImmutableList.of(
                new Deletion("file-1", 0),
                new Deletion("file-1", 1),
                new Deletion("file-2", 0),
                new Deletion("file-1", 2))));
        mergeSink.storeMergedRows(deletionsPage(ImmutableList.of(
                new Deletion("file-2", 1),
                new Deletion("file-3", 7))));

        assertThat(deletedRowsByFile(mergeSink)).isEqualTo(ImmutableMap.of(
                "file-1", ImmutableList.of(0L, 1L, 2L),
                "file-2", ImmutableList.of(0L, 1L),
                "file-3", ImmutableList.of(7L)));
    }

    @Test
    void testMemoryUsageCoversInsertSinkAndDeletions()
    {
        AtomicLong reportedBytes = new AtomicLong();
        TestingInsertPageSink insertPageSink = new TestingInsertPageSink(0);
        IcebergMergeSink mergeSink = createMergeSink(insertPageSink, reportedBytes::set);

        mergeSink.storeMergedRows(deletionsPage("file-1", 10));
        long afterFirstFile = reportedBytes.get();
        assertThat(afterFirstFile).isGreaterThan(0);

        insertPageSink.setMemoryUsage(1000);
        mergeSink.storeMergedRows(deletionsPage("file-1", 10));
        assertThat(reportedBytes.get()).isEqualTo(afterFirstFile + 1000);

        mergeSink.storeMergedRows(deletionsPage("file-2", 10));
        long afterSecondFile = reportedBytes.get();
        assertThat(afterSecondFile).isGreaterThan(afterFirstFile + 1000);

        // Additional deletions for a known file grow only its deletion vector
        DeletionVector.Builder expectedVector = DeletionVector.builder();
        for (long row = 0; row < 10; row++) {
            expectedVector.add(row);
        }
        long vectorSizeBefore = expectedVector.retainedSizeInBytes();
        for (long row = 10; row < 100_000; row++) {
            expectedVector.add(row);
        }
        mergeSink.storeMergedRows(deletionsPage("file-2", 10, 100_000));
        assertThat(reportedBytes.get() - afterSecondFile).isEqualTo(expectedVector.retainedSizeInBytes() - vectorSizeBefore);
    }

    @Test
    void testFilePathKeyDoesNotRetainInputPage()
    {
        AtomicLong reportedBytes = new AtomicLong();
        IcebergMergeSink mergeSink = createMergeSink(new TestingInsertPageSink(0), reportedBytes::set);

        Page page = deletionsPage("file-1", 5000);
        mergeSink.storeMergedRows(page);

        // The map key must not share the byte array of the file path block
        Block filePathBlock = RowBlock.getRowFieldsFromBlock(page.getBlock(3)).get(0);
        Slice blockSlice = VARCHAR.getSlice(filePathBlock, 0);
        Slice key = getOnlyElement(mergeSink.deletedFilePaths());
        assertThat(key).isEqualTo(blockSlice);
        assertThat(key.byteArray()).isNotSameAs(blockSlice.byteArray());
        assertThat(key.getRetainedSize()).isLessThan(blockSlice.getRetainedSize() / 100);
        assertThat(reportedBytes.get()).isLessThan(page.getRetainedSizeInBytes() / 10);
    }

    private static Map<String, List<Long>> deletedRowsByFile(IcebergMergeSink mergeSink)
    {
        JsonCodec<CommitTaskData> codec = JsonCodec.jsonCodec(CommitTaskData.class);
        ImmutableMap.Builder<String, List<Long>> deletedRows = ImmutableMap.builder();
        for (Slice fragment : mergeSink.finish().join()) {
            CommitTaskData task = codec.fromJson(fragment.getBytes());
            DeletionVector deletionVector = DeletionVector.builder()
                    .deserialize(Slices.wrappedBuffer(task.serializedDeletionVector().orElseThrow()))
                    .build()
                    .orElseThrow();
            ImmutableList.Builder<Long> rows = ImmutableList.builder();
            deletionVector.forEachDeletedRow(rows::add);
            deletedRows.put(task.referencedDataFile().orElseThrow(), rows.build());
        }
        return deletedRows.buildOrThrow();
    }

    private static IcebergMergeSink createMergeSink(ConnectorPageSink insertPageSink, MemoryContext memoryContext)
    {
        return new IcebergMergeSink(
                3,
                LocationProviders.locationsFor("memory:///table", ImmutableMap.of()),
                new IcebergFileWriterFactory(TESTING_TYPE_MANAGER, new NodeVersion("test"), new FileFormatDataSourceStats(), new IcebergConfig(), new OrcWriterConfig(), new ParquetWriterConfig()),
                new MemoryFileSystemFactory().create(SESSION),
                JsonCodec.jsonCodec(CommitTaskData.class),
                SESSION,
                PARQUET,
                ImmutableMap.of(),
                ImmutableMap.of(0, PartitionSpec.unpartitioned()),
                insertPageSink,
                1,
                memoryContext);
    }

    private static Page deletionsPage(String filePath, int rowCount)
    {
        return deletionsPage(filePath, 0, rowCount);
    }

    private static Page deletionsPage(String filePath, int firstRow, int endRow)
    {
        ImmutableList.Builder<Deletion> deletions = ImmutableList.builder();
        for (int row = firstRow; row < endRow; row++) {
            deletions.add(new Deletion(filePath, row));
        }
        return deletionsPage(deletions.build());
    }

    // Page with one bigint data column, the operation and case number columns and the merge row id column, every row a delete
    private static Page deletionsPage(List<Deletion> deletions)
    {
        int rowCount = deletions.size();
        BlockBuilder dataBuilder = BIGINT.createFixedSizeBlockBuilder(rowCount);
        BlockBuilder operationBuilder = TINYINT.createFixedSizeBlockBuilder(rowCount);
        BlockBuilder caseNumberBuilder = INTEGER.createFixedSizeBlockBuilder(rowCount);
        BlockBuilder filePathBuilder = VARCHAR.createBlockBuilder(null, rowCount);
        BlockBuilder rowPositionBuilder = BIGINT.createFixedSizeBlockBuilder(rowCount);
        BlockBuilder partitionSpecIdBuilder = INTEGER.createFixedSizeBlockBuilder(rowCount);
        BlockBuilder partitionDataBuilder = VARCHAR.createBlockBuilder(null, rowCount);
        BlockBuilder sourceRowIdBuilder = BIGINT.createFixedSizeBlockBuilder(rowCount);
        for (Deletion deletion : deletions) {
            BIGINT.writeLong(dataBuilder, deletion.rowPosition());
            TINYINT.writeByte(operationBuilder, (byte) DELETE_OPERATION_NUMBER);
            INTEGER.writeInt(caseNumberBuilder, 0);
            VARCHAR.writeString(filePathBuilder, deletion.filePath());
            BIGINT.writeLong(rowPositionBuilder, deletion.rowPosition());
            INTEGER.writeInt(partitionSpecIdBuilder, 0);
            VARCHAR.writeString(partitionDataBuilder, "{}");
            sourceRowIdBuilder.appendNull();
        }
        Block rowIdBlock = RowBlock.fromFieldBlocks(rowCount, new Block[] {
                filePathBuilder.build(),
                rowPositionBuilder.build(),
                partitionSpecIdBuilder.build(),
                partitionDataBuilder.build(),
                sourceRowIdBuilder.build(),
        });
        return new Page(dataBuilder.build(), operationBuilder.build(), caseNumberBuilder.build(), rowIdBlock);
    }

    private record Deletion(String filePath, long rowPosition) {}

    private static final class TestingInsertPageSink
            implements ConnectorPageSink
    {
        private long memoryUsage;

        private TestingInsertPageSink(long memoryUsage)
        {
            this.memoryUsage = memoryUsage;
        }

        public void setMemoryUsage(long memoryUsage)
        {
            this.memoryUsage = memoryUsage;
        }

        @Override
        public long getMemoryUsage()
        {
            return memoryUsage;
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            return NOT_BLOCKED;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            return completedFuture(ImmutableList.of());
        }

        @Override
        public void abort() {}
    }
}
