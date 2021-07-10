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
import io.trino.orc.OrcReaderOptions;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static org.testng.Assert.assertEquals;

public class TestOrcDeletedRows
{
    private Path partitionDirectory;
    private Block rowIdBlock;
    private Block bucketBlock;

    @BeforeClass
    public void setUp()
    {
        partitionDirectory = new Path(TestOrcDeletedRows.class.getClassLoader().getResource("fullacid_delete_delta_test") + "/");
        rowIdBlock = BIGINT.createFixedSizeBlockBuilder(1)
                .writeLong(0)
                .build();
        bucketBlock = INTEGER.createFixedSizeBlockBuilder(1)
                .writeInt(536870912)
                .build();
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

        assertEquals(validRows.size(), 8);
        assertEquals(validRows, ImmutableSet.of(0L, 1L, 3L, 4L, 5L, 7L, 8L, 9L));

        // page with no deleted rows
        testPage = createTestPage(10, 20);
        block = deletedRows.getMaskDeletedRowsFunction(testPage, OptionalLong.empty()).apply(testPage.getBlock(1));
        assertEquals(block.getPositionCount(), 10);
    }

    @Test
    public void testDeletedLocationsOriginalFiles()
    {
        Path path = new Path(TestOrcDeletedRows.class.getClassLoader().getResource("dummy_id_data_orc") + "/");
        AcidInfo.Builder acidInfoBuilder = AcidInfo.builder(path);
        addDeleteDelta(acidInfoBuilder, 10000001L, 10000001L, OptionalInt.of(0), path);

        acidInfoBuilder.addOriginalFile(new Path(path, "000000_0"), 743, 0);
        acidInfoBuilder.addOriginalFile(new Path(path, "000001_0"), 730, 0);

        OrcDeletedRows deletedRows = createOrcDeletedRows(acidInfoBuilder.buildWithRequiredOriginalFiles(0), "000000_0");

        // page with deleted rows
        Page testPage = createTestPage(0, 8);
        Block block = deletedRows.getMaskDeletedRowsFunction(testPage, OptionalLong.of(0L)).apply(testPage.getBlock(0));
        Set<Object> validRows = resultBuilder(SESSION, BIGINT)
                .page(new Page(block))
                .build()
                .getOnlyColumnAsSet();

        assertEquals(validRows.size(), 7);
        assertEquals(validRows, ImmutableSet.of(0L, 1L, 3L, 4L, 5L, 6L, 7L));

        // page with no deleted rows
        testPage = createTestPage(5, 9);
        block = deletedRows.getMaskDeletedRowsFunction(testPage, OptionalLong.empty()).apply(testPage.getBlock(1));
        assertEquals(block.getPositionCount(), 4);
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

        assertEquals(validRows.size(), 9);
        assertEquals(validRows, ImmutableSet.of(0L, 1L, 3L, 4L, 5L, 6L, 7L, 8L, 9L));

        // page with no deleted rows
        testPage = createTestPage(10, 20);
        block = deletedRows.getMaskDeletedRowsFunction(testPage, OptionalLong.empty()).apply(testPage.getBlock(1));
        assertEquals(block.getPositionCount(), 10);
    }

    private void addDeleteDelta(AcidInfo.Builder acidInfoBuilder, long minWriteId, long maxWriteId, OptionalInt statementId, Path path)
    {
        Path deleteDeltaPath;
        if (statementId.isPresent()) {
            deleteDeltaPath = new Path(path, AcidUtils.deleteDeltaSubdir(minWriteId, maxWriteId, statementId.getAsInt()));
        }
        else {
            deleteDeltaPath = new Path(path, AcidUtils.deleteDeltaSubdir(minWriteId, maxWriteId));
        }
        acidInfoBuilder.addDeleteDelta(deleteDeltaPath);
    }

    private static OrcDeletedRows createOrcDeletedRows(AcidInfo acidInfo, String sourceFileName)
    {
        JobConf configuration = new JobConf(new Configuration(false));
        OrcDeleteDeltaPageSourceFactory pageSourceFactory = new OrcDeleteDeltaPageSourceFactory(
                new OrcReaderOptions(),
                "test",
                configuration,
                HDFS_ENVIRONMENT,
                new FileFormatDataSourceStats());

        return new OrcDeletedRows(
                sourceFileName,
                pageSourceFactory,
                "test",
                configuration,
                HDFS_ENVIRONMENT,
                acidInfo,
                OptionalInt.of(0));
    }

    private Page createTestPage(int originalTransactionStart, int originalTransactionEnd)
    {
        int size = originalTransactionEnd - originalTransactionStart;
        BlockBuilder originalTransaction = BIGINT.createFixedSizeBlockBuilder(size);
        for (long i = originalTransactionStart; i < originalTransactionEnd; i++) {
            originalTransaction.writeLong(i);
        }

        return new Page(
                size,
                originalTransaction.build(),
                new RunLengthEncodedBlock(bucketBlock, size),
                new RunLengthEncodedBlock(rowIdBlock, size));
    }
}
