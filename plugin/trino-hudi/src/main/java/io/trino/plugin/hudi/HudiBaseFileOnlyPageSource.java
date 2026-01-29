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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.util.SynthesizedColumnHandler;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * This page source is for reading data columns in the parquet format.
 * This page source also avoids costly avro IndexRecord serialization.
 */
public class HudiBaseFileOnlyPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource dataPageSource;
    private final List<HiveColumnHandle> allOutputColumns;
    private final SynthesizedColumnHandler synthesizedColumnHandler;
    // Maps output channel to physical source channel, or -1 if synthesized
    private final int[] physicalSourceChannelMap;

    public HudiBaseFileOnlyPageSource(
            ConnectorPageSource dataPageSource,
            List<HiveColumnHandle> allOutputColumns,
            // Columns provided by dataPageSource
            List<HiveColumnHandle> dataColumns,
            // Handler to manage synthesized/virtual in Hudi tables such as partition columns and metadata, i.e. file size (not hudi metadata)
            SynthesizedColumnHandler synthesizedColumnHandler)
    {
        this.dataPageSource = requireNonNull(dataPageSource, "dataPageSource is null");
        this.allOutputColumns = ImmutableList.copyOf(requireNonNull(allOutputColumns, "allOutputColumns is null"));
        this.synthesizedColumnHandler = requireNonNull(synthesizedColumnHandler, "synthesizedColumnHandler is null");

        // Create a mapping from the channel index in the output page to the channel index in the physicalDataPageSource's page
        this.physicalSourceChannelMap = new int[allOutputColumns.size()];
        Map<String, Integer> physicalColumnNameToChannel = new HashMap<>();
        for (int i = 0; i < dataColumns.size(); i++) {
            physicalColumnNameToChannel.put(dataColumns.get(i).getName().toLowerCase(Locale.ENGLISH), i);
        }

        for (int i = 0; i < allOutputColumns.size(); i++) {
            this.physicalSourceChannelMap[i] = physicalColumnNameToChannel.getOrDefault(allOutputColumns.get(i).getName().toLowerCase(Locale.ENGLISH), -1);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return dataPageSource.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return dataPageSource.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return dataPageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return dataPageSource.isFinished();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        SourcePage physicalSourcePage = dataPageSource.getNextSourcePage();
        if (physicalSourcePage == null) {
            return null;
        }

        int positionCount = physicalSourcePage.getPositionCount();
        if (positionCount == 0 && synthesizedColumnHandler.getSynthesizedColumnCount() == 0) {
            // If only physical columns and page is empty
            return physicalSourcePage;
        }

        return new HudiSourcePage(physicalSourcePage, allOutputColumns, physicalSourceChannelMap, synthesizedColumnHandler);
    }

    @Override
    public long getMemoryUsage()
    {
        return dataPageSource.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        dataPageSource.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return dataPageSource.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return dataPageSource.getMetrics();
    }

    private record HudiSourcePage(
            SourcePage sourcePage,
            List<HiveColumnHandle> allOutputColumns,
            int[] physicalSourceChannelMap,
            SynthesizedColumnHandler synthesizedColumnHandler,
            Block[] blocks)
            implements SourcePage
    {
        private static final long INSTANCE_SIZE = instanceSize(HudiSourcePage.class);

        private HudiSourcePage(
                SourcePage sourcePage,
                List<HiveColumnHandle> allOutputColumns,
                int[] physicalSourceChannelMap,
                SynthesizedColumnHandler synthesizedColumnHandler)
        {
            this(sourcePage, allOutputColumns, physicalSourceChannelMap, synthesizedColumnHandler, new Block[allOutputColumns.size()]);
        }

        private HudiSourcePage
        {
            requireNonNull(sourcePage, "sourcePage is null");
            allOutputColumns = ImmutableList.copyOf(requireNonNull(allOutputColumns, "allOutputColumns is null"));
            requireNonNull(physicalSourceChannelMap, "physicalSourceChannelMap is null");
            requireNonNull(synthesizedColumnHandler, "synthesizedColumnHandler is null");
            requireNonNull(blocks, "blocks is null");
        }

        @Override
        public int getPositionCount()
        {
            return sourcePage.getPositionCount();
        }

        @Override
        public long getSizeInBytes()
        {
            return sourcePage.getSizeInBytes();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE +
                    sizeOf(blocks) +
                    sizeOf(physicalSourceChannelMap) +
                    sourcePage.getRetainedSizeInBytes();
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            consumer.accept(this, INSTANCE_SIZE);
            consumer.accept(blocks, sizeOf(blocks));
            consumer.accept(physicalSourceChannelMap, sizeOf(physicalSourceChannelMap));
            for (Block block : blocks) {
                if (block != null) {
                    block.retainedBytesForEachPart(consumer);
                }
            }
            sourcePage.retainedBytesForEachPart(consumer);
        }

        @Override
        public int getChannelCount()
        {
            return blocks.length;
        }

        @Override
        public Block getBlock(int channel)
        {
            Block block = blocks[channel];
            if (block == null) {
                HiveColumnHandle outputColumn = allOutputColumns.get(channel);
                if (physicalSourceChannelMap[channel] != -1) {
                    block = sourcePage.getBlock(physicalSourceChannelMap[channel]);
                }
                else {
                    // Column is synthesized
                    block = synthesizedColumnHandler.createRleSynthesizedBlock(outputColumn, sourcePage.getPositionCount());
                }
                blocks[channel] = block;
            }
            return block;
        }

        @Override
        public Page getPage()
        {
            for (int i = 0; i < blocks.length; i++) {
                getBlock(i);
            }
            return new Page(getPositionCount(), blocks);
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            sourcePage.selectPositions(positions, offset, size);
            for (int i = 0; i < blocks.length; i++) {
                Block block = blocks[i];
                if (block != null) {
                    blocks[i] = block.getPositions(positions, offset, size);
                }
            }
        }
    }
}
