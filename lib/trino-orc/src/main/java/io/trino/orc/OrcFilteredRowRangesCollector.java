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
package io.trino.orc;

import io.trino.spi.connector.ConnectorPageSource;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import jakarta.annotation.Nullable;

public class OrcFilteredRowRangesCollector
{
    private final LongArrayList lowerInclusive = new LongArrayList();
    private final LongArrayList upperExclusive = new LongArrayList();
    private boolean noMoreRowRanges;

    public ConnectorPageSource.RowRanges collectRowRanges()
    {
        long[] lowerInclusive = this.lowerInclusive.toLongArray();
        this.lowerInclusive.clear();
        long[] upperExclusive = this.upperExclusive.toLongArray();
        this.upperExclusive.clear();
        return new ConnectorPageSource.RowRanges(lowerInclusive, upperExclusive, noMoreRowRanges);
    }

    public void addStripe(@Nullable Stripe stripe, long stripeSplitPosition, boolean noMoreRowRanges)
    {
        this.noMoreRowRanges = noMoreRowRanges;
        if (stripe == null) {
            return;
        }
        for (RowGroup rowGroup : stripe.getRowGroups()) {
            addRowRange(stripeSplitPosition + rowGroup.getRowOffset(), stripeSplitPosition + rowGroup.getRowOffset() + rowGroup.getRowCount());
        }
    }

    private void addRowRange(long startInclusive, long endExclusive)
    {
        // union with last range if possible
        if (!upperExclusive.isEmpty() && getLastElement(upperExclusive) == startInclusive) {
            removeLastElement(upperExclusive);
            upperExclusive.add(endExclusive);
            return;
        }
        lowerInclusive.add(startInclusive);
        upperExclusive.add(endExclusive);
    }

    private static long getLastElement(LongArrayList arrayList)
    {
        return arrayList.getLong(arrayList.size() - 1);
    }

    private static void removeLastElement(LongArrayList arrayList)
    {
        arrayList.removeLong(arrayList.size() - 1);
    }
}
