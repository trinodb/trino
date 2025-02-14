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
package io.trino.plugin.hive.orc;

import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.orc.OrcReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.AcidInfo;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.junit.jupiter.api.Test;

import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.io.Resources.getResource;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOrcDeletedRows
{
    private final Location partitionDirectory;
    private final Block rowIdBlock;
    private final Block bucketBlock;

    public TestOrcDeletedRows()
    {
        partitionDirectory = Location.of(getResource("fullacid_delete_delta_test").toString());

        BlockBuilder rowIdBlockBuilder = BIGINT.createFixedSizeBlockBuilder(1);
        BIGINT.writeLong(rowIdBlockBuilder, 0);
        rowIdBlock = rowIdBlockBuilder.build();

        BlockBuilder bucketBlockBuilder = INTEGER.createFixedSizeBlockBuilder(1);
        INTEGER.writeInt(bucketBlockBuilder, 536870912);
        bucketBlock = bucketBlockBuilder.build();
    }

    @Test
    public void testDeleteLocations()
    {
        AcidInfo.Builder acidInfoBuilder = AcidInfo.builder(partitionDirectory);
        addDeleteDelta(acidInfoBuilder, 4L, 4L, OptionalInt.of(0), partitionDirectory);
        addDeleteDelta(acidInfoBuilder, 7L, 7L, OptionalInt.of(0), partitionDirectory);

        OrcDeletedRows deletedRows = createOrcDeletedRows(acidInfoBuilder.build().orElseThrow(), "bucket_00000");

        // page with deleted rows
        Page testPage = createTestPage(0, 10);
        Block block = deletedRows.getMaskDeletedRowsFunction(testPage, OptionalLong.empty()).apply(testPage.getBlock(0));
        Set<Object> validRows = resultBuilder(SESSION, BIGINT)
                .page(new Page(block))
                .build()
                .getOnlyColumnAsSet();

        assertThat(validRows).hasSize(8);
        assertThat(validRows).isEqualTo(ImmutableSet.of(0L, 1L, 3L, 4L, 5L, 7L, 8L, 9L));

        // page with no deleted rows
        testPage = createTestPage(10, 20);
        block = deletedRows.getMaskDeletedRowsFunction(testPage, OptionalLong.empty()).apply(testPage.getBlock(1));
        assertThat(block.getPositionCount()).isEqualTo(10);
    }

    @Test
    public void testDeletedLocationsOriginalFiles()
    {
        Location path = Location.of(getResource("dummy_id_data_orc").toString());
        AcidInfo.Builder acidInfoBuilder = AcidInfo.builder(path);
        addDeleteDelta(acidInfoBuilder, 10000001L, 10000001L, OptionalInt.of(0), path);

        acidInfoBuilder.addOriginalFile(path.appendPath("000000_0"), 743, 0);
        acidInfoBuilder.addOriginalFile(path.appendPath("000001_0"), 730, 0);

        OrcDeletedRows deletedRows = createOrcDeletedRows(acidInfoBuilder.buildWithRequiredOriginalFiles(0), "000000_0");

        // page with deleted rows
        Page testPage = createTestPage(0, 8);
        Block block = deletedRows.getMaskDeletedRowsFunction(testPage, OptionalLong.of(0L)).apply(testPage.getBlock(0));
        Set<Object> validRows = resultBuilder(SESSION, BIGINT)
                .page(new Page(block))
                .build()
                .getOnlyColumnAsSet();

        assertThat(validRows).hasSize(7);
        assertThat(validRows).isEqualTo(ImmutableSet.of(0L, 1L, 3L, 4L, 5L, 6L, 7L));

        // page with no deleted rows
        testPage = createTestPage(5, 9);
        block = deletedRows.getMaskDeletedRowsFunction(testPage, OptionalLong.empty()).apply(testPage.getBlock(1));
        assertThat(block.getPositionCount()).isEqualTo(4);
    }

    @Test
    public void testDeletedLocationsAfterMinorCompaction()
    {
        AcidInfo.Builder acidInfoBuilder = AcidInfo.builder(partitionDirectory);
        addDeleteDelta(acidInfoBuilder, 4L, 4L, OptionalInt.empty(), partitionDirectory);

        OrcDeletedRows deletedRows = createOrcDeletedRows(acidInfoBuilder.build().orElseThrow(), "bucket_00000");

        // page with deleted rows
        Page testPage = createTestPage(0, 10);
        Block block = deletedRows.getMaskDeletedRowsFunction(testPage, OptionalLong.empty()).apply(testPage.getBlock(0));
        Set<Object> validRows = resultBuilder(SESSION, BIGINT)
                .page(new Page(block))
                .build()
                .getOnlyColumnAsSet();

        assertThat(validRows).hasSize(9);
        assertThat(validRows).isEqualTo(ImmutableSet.of(0L, 1L, 3L, 4L, 5L, 6L, 7L, 8L, 9L));

        // page with no deleted rows
        testPage = createTestPage(10, 20);
        block = deletedRows.getMaskDeletedRowsFunction(testPage, OptionalLong.empty()).apply(testPage.getBlock(1));
        assertThat(block.getPositionCount()).isEqualTo(10);
    }

    private static void addDeleteDelta(AcidInfo.Builder acidInfoBuilder, long minWriteId, long maxWriteId, OptionalInt statementId, Location path)
    {
        String subdir = statementId.stream()
                .mapToObj(id -> AcidUtils.deleteDeltaSubdir(minWriteId, maxWriteId, id))
                .findFirst()
                .orElseGet(() -> AcidUtils.deleteDeltaSubdir(minWriteId, maxWriteId));
        acidInfoBuilder.addDeleteDelta(path.appendPath(subdir));
    }

    private static OrcDeletedRows createOrcDeletedRows(AcidInfo acidInfo, String sourceFileName)
    {
        OrcDeleteDeltaPageSourceFactory pageSourceFactory = new OrcDeleteDeltaPageSourceFactory(
                new OrcReaderOptions(),
                new FileFormatDataSourceStats());

        OrcDeletedRows deletedRows = new OrcDeletedRows(
                sourceFileName,
                pageSourceFactory,
                ConnectorIdentity.ofUser("test"),
                HDFS_FILE_SYSTEM_FACTORY,
                acidInfo,
                OptionalInt.of(0),
                newSimpleAggregatedMemoryContext());

        // ensure deletedRows is loaded
        while (!deletedRows.loadOrYield()) {
            // do nothing
        }

        return deletedRows;
    }

    private Page createTestPage(int originalTransactionStart, int originalTransactionEnd)
    {
        int size = originalTransactionEnd - originalTransactionStart;
        BlockBuilder originalTransaction = BIGINT.createFixedSizeBlockBuilder(size);
        for (long i = originalTransactionStart; i < originalTransactionEnd; i++) {
            BIGINT.writeLong(originalTransaction, i);
        }

        return new Page(
                size,
                originalTransaction.build(),
                RunLengthEncodedBlock.create(bucketBlock, size),
                RunLengthEncodedBlock.create(rowIdBlock, size));
    }
}
