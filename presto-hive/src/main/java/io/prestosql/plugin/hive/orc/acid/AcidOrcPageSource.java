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
package io.prestosql.plugin.hive.orc.acid;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.orc.OrcPageSource;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LazyBlockLoader;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.orc.AcidConstants.PRESTO_ACID_BUCKET_INDEX;
import static io.prestosql.orc.AcidConstants.PRESTO_ACID_META_COLS_COUNT;
import static io.prestosql.orc.AcidConstants.PRESTO_ACID_ORIGINAL_TRANSACTION_INDEX;
import static io.prestosql.orc.AcidConstants.PRESTO_ACID_ROWID_INDEX;
import static io.prestosql.spi.block.DictionaryId.randomDictionaryId;

public class AcidOrcPageSource
        extends OrcPageSource
{
    private DeletedRowsRegistry deletedRowsRegistry;

    public AcidOrcPageSource(
            Path splitPath,
            OrcPageSourceFactory pageSourceFactory,
            ConnectorSession session,
            Configuration configuration,
            DateTimeZone hiveStorageTimeZone,
            HdfsEnvironment hdfsEnvironment,
            OrcRecordReader recordReader,
            OrcDataSource orcDataSource,
            ImmutableMap<Integer, Type> includedColumns,
            AggregatedMemoryContext systemMemoryContext,
            FileFormatDataSourceStats stats,
            DataSize deletedRowsCacheSize,
            Duration deletedRowsCacheTtl,
            Optional<DeleteDeltaLocations> deleteDeltaLocations)
            throws ExecutionException
    {
        super(recordReader, orcDataSource, includedColumns, systemMemoryContext, stats);

        checkState(
                deleteDeltaLocations.map(DeleteDeltaLocations::hadDeletedRows).orElse(false),
                "OrcPageSource should be used when there are no deleted rows");

        deletedRowsRegistry = new DeletedRowsRegistry(
                splitPath,
                pageSourceFactory,
                session,
                configuration,
                hiveStorageTimeZone,
                hdfsEnvironment,
                deletedRowsCacheSize,
                deletedRowsCacheTtl,
                deleteDeltaLocations);
    }

    @Override
    public Page getNextPage()
    {
        Page page = super.getNextPage();

        if (page == null) {
            return null;
        }

        ValidPositions validPositions = deletedRowsRegistry.getValidPositions(
                page.getPositionCount(),
                page.getBlock(PRESTO_ACID_ORIGINAL_TRANSACTION_INDEX),
                page.getBlock(PRESTO_ACID_BUCKET_INDEX),
                page.getBlock(PRESTO_ACID_ROWID_INDEX));
        Block[] dataBlocks = new Block[page.getChannelCount() - PRESTO_ACID_META_COLS_COUNT];
        int colIdx = 0;
        for (int i = PRESTO_ACID_META_COLS_COUNT; i < page.getChannelCount(); i++) {
            // LazyBlock is required to prevent DictionaryBlock from loading the dictionary block in constructor via getRetainedSize
            dataBlocks[colIdx++] = new LazyBlock(validPositions.getPositionCount(), new AcidOrcBlockLoader(validPositions, page.getBlock(i)));
        }

        return new Page(validPositions.getPositionCount(), dataBlocks);
    }

    private final class AcidOrcBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final ValidPositions validPositions;
        private final Block sourceBlock;
        private boolean loaded;

        public AcidOrcBlockLoader(ValidPositions validPositions, Block sourceBlock)
        {
            this.validPositions = validPositions;
            this.sourceBlock = sourceBlock;
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            checkState(!loaded, "Already loaded");

            Block block = new DictionaryBlock(0, validPositions.getPositionCount(), sourceBlock, validPositions.getValidPositions(), false, randomDictionaryId());
            lazyBlock.setBlock(block);

            loaded = true;
        }
    }
}
