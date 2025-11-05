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
package io.trino.plugin.faker;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import static java.lang.Math.min;

public class FakerSplitManager
        implements ConnectorSplitManager
{
    // this is equal to 250 pages generated in FakerPageSource
    static final long MAX_ROWS_PER_SPLIT = 250 * 4096;

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        FakerTableHandle fakerTable = (FakerTableHandle) table;
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        int splitNum = 0;
        long rowsOffset = 0;
        long remainingRows = fakerTable.limit();
        while (remainingRows > 0) {
            long splitSize = min(remainingRows, MAX_ROWS_PER_SPLIT);
            splits.add(new FakerSplit(splitNum, rowsOffset, splitSize));
            rowsOffset += splitSize;
            remainingRows -= splitSize;
            splitNum++;
        }
        return new FixedSplitSource(splits.build());
    }
}
