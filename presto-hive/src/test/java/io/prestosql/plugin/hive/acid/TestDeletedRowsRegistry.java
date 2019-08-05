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
package io.prestosql.plugin.hive.acid;

import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.plugin.hive.orc.acid.DeletedRowsRegistry;
import io.prestosql.plugin.hive.orc.acid.ValidPositions;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.plugin.hive.HiveTestUtils.SESSION;
import static io.prestosql.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.prestosql.plugin.hive.acid.AcidPageProcessorProvider.CONFIG;
import static io.prestosql.plugin.hive.acid.AcidPageProcessorProvider.ORC_CONFIG;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static org.testng.Assert.assertTrue;

public class TestDeletedRowsRegistry
{
    // ClassLoader finds top level resources, find that and build delta locations from it
    private final File partitionLocation = new File((Thread.currentThread().getContextClassLoader().getResource("fullacid_delete_delta_test").getPath()));

    @Test
    public void testReadingDeletedRows()
            throws ExecutionException
    {
        DeleteDeltaLocations.Builder deleteDeltaLocationsBuilder = new DeleteDeltaLocations.Builder(new Path(partitionLocation.toString()));
        addDeleteDelta(deleteDeltaLocationsBuilder, 4L, 4L, 0);
        addDeleteDelta(deleteDeltaLocationsBuilder, 7L, 7L, 0);
        // Delete Delta files are named bucket_00000, pass the split path as same name
        Path inputSplitPath = new Path("file:///tmp/bucket_00000");
        DeletedRowsRegistry registry = createDeletedRowsRegistry(inputSplitPath, Optional.of(deleteDeltaLocationsBuilder.build()));
        Set<DeletedRowsRegistry.RowId> deletedRows = registry.loadDeletedRows();
        assertTrue(deletedRows.size() == 2, "Expected to read 2 deleted rows but read: " + deletedRows);
        assertTrue(deletedRows.contains(new DeletedRowsRegistry.RowId(2L, 536870912, 0L)), "RowId{2, 536870912, 0} not found in: " + deletedRows);
        assertTrue(deletedRows.contains(new DeletedRowsRegistry.RowId(6L, 536870912, 0L)), "RowId{6, 536870912, 0} not found in: " + deletedRows);
    }

    @Test
    public void testValidPositionsWithDeletedRows()
            throws ExecutionException
    {
        DeleteDeltaLocations.Builder deleteDeltaLocationsBuilder = new DeleteDeltaLocations.Builder(new Path(partitionLocation.toString()));
        addDeleteDelta(deleteDeltaLocationsBuilder, 4L, 4L, 0);
        addDeleteDelta(deleteDeltaLocationsBuilder, 7L, 7L, 0);
        // Delete Delta files are named bucket_00000, pass the split path as same name
        Path inputSplitPath = new Path("file:///tmp/bucket_00000");
        DeletedRowsRegistry registry = createDeletedRowsRegistry(inputSplitPath, Optional.of(deleteDeltaLocationsBuilder.build()));

        int size = 10;
        BlockBuilder originalTransaction = BIGINT.createFixedSizeBlockBuilder(size);
        for (long i = 0; i < size; i++) {
            originalTransaction.writeLong(i);
        }

        BlockBuilder bucket = INTEGER.createFixedSizeBlockBuilder(size);
        for (int i = 0; i < size; i++) {
            bucket.writeInt(536870912);
        }

        BlockBuilder rowId = BIGINT.createFixedSizeBlockBuilder(size);
        for (long i = 0; i < size; i++) {
            rowId.writeLong(0L);
        }

        // 2 rows should be deleted RowId{2, 536870912, 0} and RowId{6, 536870912, 0}
        ValidPositions validPositions = registry.getValidPositions(size, originalTransaction.build(), bucket.build(), rowId.build());
        assertTrue(validPositions.getPositionCount() == 8, "Unexpected number of valid positions found: " + validPositions.getPositionCount());

        // Verify except row 2 and row 6 all others are marked valid
        int validPositionIndex = 0;
        for (int i = 0; i < size; i++) {
            if (i != 2 && i != 6) {
                assertTrue(validPositions.getPosition(validPositionIndex++) == i, "Row number " + i + " should have been marked valid");
            }
        }
    }

    @Test
    public void testValidPositionsWithNoDeletedRowsFailCase()
            throws ExecutionException
    {
        Path inputSplitPath = new Path("file:///tmp/bucket_00000");
        DeletedRowsRegistry registry = createDeletedRowsRegistry(inputSplitPath, Optional.empty());
        int size = 100;
        Block originalTransaction = fillDummyValues(BIGINT.createFixedSizeBlockBuilder(size), size);
        Block bucket = fillDummyValues(INTEGER.createFixedSizeBlockBuilder(size), size);
        Block rowID = fillDummyValues(BIGINT.createFixedSizeBlockBuilder(size), size);
        try {
            ValidPositions validPositions = registry.getValidPositions(size, originalTransaction, bucket, rowID);
        }
        catch (IllegalStateException e) {
            // Valid case
            return;
        }
        assertTrue(false, "Test should have failed as we asked for valid positions when there are no deleted rows");
    }

    private Block fillDummyValues(BlockBuilder blockBuilder, int size)
    {
        for (int i = 0; i < size; i++) {
            blockBuilder.appendNull();
        }
        return blockBuilder.build();
    }

    private void addDeleteDelta(DeleteDeltaLocations.Builder deleteDeltaLocationsBuilder, long minWriteId, long maxWriteId, int statementId)
    {
        Path deleteDeltaPath = new Path(new Path(partitionLocation.toString()), AcidUtils.deleteDeltaSubdir(minWriteId, maxWriteId, statementId));
        deleteDeltaLocationsBuilder.addDeleteDelta(deleteDeltaPath, minWriteId, maxWriteId, statementId);
    }

    private DeletedRowsRegistry createDeletedRowsRegistry(Path inputSplitPath, Optional<DeleteDeltaLocations> deleteDeltaLocations)
            throws ExecutionException
    {
        OrcPageSourceFactory orcPageSourceFactory = new OrcPageSourceFactory(TYPE_MANAGER, ORC_CONFIG, HDFS_ENVIRONMENT, new FileFormatDataSourceStats());

        Configuration config = new JobConf(new Configuration(false));
        config.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        return new DeletedRowsRegistry(
                inputSplitPath,
                orcPageSourceFactory,
                SESSION,
                config,
                DateTimeZone.forID(SESSION.getTimeZoneKey().getId()),
                HDFS_ENVIRONMENT,
                CONFIG.getDeleteDeltaCacheSize(),
                CONFIG.getDeleteDeltaCacheTtl(),
                deleteDeltaLocations);
    }
}
