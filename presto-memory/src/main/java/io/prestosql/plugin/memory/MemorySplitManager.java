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
package io.prestosql.plugin.memory;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.util.List;
import java.util.OptionalLong;

public final class MemorySplitManager
        implements ConnectorSplitManager
{
    private final int splitsPerNode;
    private final MemoryMetadata metadata;

    @Inject
    public MemorySplitManager(MemoryConfig config, MemoryMetadata metadata)
    {
        this.splitsPerNode = config.getSplitsPerNode();
        this.metadata = metadata;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableHandle handle, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        MemoryTableHandle table = (MemoryTableHandle) handle;

        List<MemoryDataFragment> dataFragments = metadata.getDataFragments(table.getId());

        int totalRows = 0;

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        for (MemoryDataFragment dataFragment : dataFragments) {
            long rows = dataFragment.getRows();
            totalRows += rows;

            if (table.getLimit().isPresent() && totalRows > table.getLimit().getAsLong()) {
                rows -= totalRows - table.getLimit().getAsLong();
                splits.add(new MemorySplit(table.getId(), 0, 1, dataFragment.getHostAddress(), rows, OptionalLong.of(rows)));
                break;
            }

            for (int i = 0; i < splitsPerNode; i++) {
                splits.add(new MemorySplit(table.getId(), i, splitsPerNode, dataFragment.getHostAddress(), rows, OptionalLong.empty()));
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
