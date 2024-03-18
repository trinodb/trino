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
package io.trino.plugin.varada.storage.read;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.varada.dispatcher.RowGroupCloseHandler;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.query.data.collect.PrefilledQueryCollectData;
import io.trino.plugin.warp.gen.stats.VaradaStatsDispatcherPageSource;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PrefilledPageSource
        implements ConnectorPageSource
{
    private final ImmutableMap<Integer, PrefilledQueryCollectData> prefilledQueryCollectDataByBlockIndex;
    private final VaradaStatsDispatcherPageSource stats;

    private final long startTime;
    private final RowGroupData rowGroupData;

    private final int totalRecords;
    private final Optional<RowGroupCloseHandler> closeHandler;
    private boolean finished;

    public PrefilledPageSource(Map<Integer, PrefilledQueryCollectData> prefilledQueryCollectDataByBlockIndex,
            VaradaStatsDispatcherPageSource stats,
            RowGroupData rowGroupData,
            int totalRecords,
            Optional<RowGroupCloseHandler> closeHandler)
    {
        this.prefilledQueryCollectDataByBlockIndex = ImmutableMap.copyOf(requireNonNull(prefilledQueryCollectDataByBlockIndex));
        this.stats = requireNonNull(stats);
        this.rowGroupData = requireNonNull(rowGroupData);
        this.totalRecords = totalRecords;
        this.closeHandler = requireNonNull(closeHandler);
        startTime = System.currentTimeMillis();
    }

    public boolean hasBlock(int blockIndex)
    {
        return prefilledQueryCollectDataByBlockIndex.containsKey(blockIndex);
    }

    public Block createBlock(int blockIndex, int positionCount)
    {
        PrefilledQueryCollectData prefilledQueryCollectData = prefilledQueryCollectDataByBlockIndex.get(blockIndex);
        if (prefilledQueryCollectData == null) {
            throw new RuntimeException("blockIndex does not exists");
        }

        Block singleBlock = prefilledQueryCollectData.getSingleValue().getValueBlock();
        stats.addprefilled_collect_bytes(positionCount * singleBlock.getSizeInBytes());
        return RunLengthEncodedBlock.create(singleBlock, positionCount);
    }

    public int getChannelCount()
    {
        return prefilledQueryCollectDataByBlockIndex.size();
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        Page page;
        if (prefilledQueryCollectDataByBlockIndex.isEmpty()) {
            PageBuilder pageBuilder = new PageBuilder(Collections.emptyList());
            pageBuilder.declarePositions(totalRecords);
            page = pageBuilder.build();
        }
        else {
            page = new Page(prefilledQueryCollectDataByBlockIndex.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> createBlock(entry.getValue().getBlockIndex(), totalRecords))
                    .toArray(Block[]::new));
        }
        finished = true;
        return page;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        closeHandler.ifPresent(handler -> handler.accept(rowGroupData));
        stats.inccached_varada_success_files();
        stats.addexecution_time(System.currentTimeMillis() - this.startTime);
    }
}
