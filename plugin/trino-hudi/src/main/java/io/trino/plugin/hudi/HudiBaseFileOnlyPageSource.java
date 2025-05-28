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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
    public Page getNextPage()
    {
        Page physicalSourcePage = dataPageSource.getNextPage();
        if (physicalSourcePage == null) {
            return null;
        }

        int positionCount = physicalSourcePage.getPositionCount();
        if (positionCount == 0 && synthesizedColumnHandler.getSynthesizedColumnCount() == 0) {
            // If only physical columns and page is empty
            return physicalSourcePage;
        }

        Block[] outputBlocks = new Block[allOutputColumns.size()];
        for (int i = 0; i < allOutputColumns.size(); i++) {
            HiveColumnHandle outputColumn = allOutputColumns.get(i);
            if (physicalSourceChannelMap[i] != -1) {
                outputBlocks[i] = physicalSourcePage.getBlock(physicalSourceChannelMap[i]);
            }
            else {
                // Column is synthesized
                outputBlocks[i] = synthesizedColumnHandler.createRleSynthesizedBlock(outputColumn, positionCount);
            }
        }
        return new Page(outputBlocks);
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
}
