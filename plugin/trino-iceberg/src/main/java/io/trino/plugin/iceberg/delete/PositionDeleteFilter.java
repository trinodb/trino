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
package io.trino.plugin.iceberg.delete;

import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.roaringbitmap.longlong.ImmutableLongBitmapDataProvider;
import org.roaringbitmap.longlong.LongBitmapDataProvider;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public final class PositionDeleteFilter
        implements DeleteFilter
{
    private final ImmutableLongBitmapDataProvider deletedRows;

    public PositionDeleteFilter(ImmutableLongBitmapDataProvider deletedRows)
    {
        this.deletedRows = requireNonNull(deletedRows, "deletedRows is null");
    }

    @Override
    public RowPredicate createPredicate(List<IcebergColumnHandle> columns, long dataSequenceNumber)
    {
        int filePosChannel = rowPositionChannel(columns);
        return (page, position) -> {
            Block block = page.getBlock(filePosChannel);
            long filePos = BIGINT.getLong(block, position);
            return !deletedRows.contains(filePos);
        };
    }

    private static int rowPositionChannel(List<IcebergColumnHandle> columns)
    {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).isRowPositionColumn()) {
                return i;
            }
        }
        throw new IllegalArgumentException("No row position column");
    }

    public static void readPositionDeletes(ConnectorPageSource pageSource, Slice targetPath, LongBitmapDataProvider deletedRows)
    {
        CachingVarcharComparator comparator = new CachingVarcharComparator(targetPath);

        // Use a linear search since we expect most deletion files to only contain
        // entries for a single path. The comparison cost is minimal due if the
        // path values are dictionary encoded, since we only do the comparison once.
        while (!pageSource.isFinished()) {
            SourcePage page = pageSource.getNextSourcePage();
            if (page == null) {
                continue;
            }

            Block pathBlock = page.getBlock(0);
            Block posBlock = page.getBlock(1);

            for (int position = 0; position < page.getPositionCount(); position++) {
                int result = comparator.compare(pathBlock, position);
                if (result > 0) {
                    // deletion files are sorted by path, so we're done
                    return;
                }
                if (result == 0) {
                    deletedRows.addLong(BIGINT.getLong(posBlock, position));
                }
            }
        }
    }

    private static final class CachingVarcharComparator
    {
        private final Slice reference;
        private int result;
        private Slice value;

        public CachingVarcharComparator(Slice reference)
        {
            this.reference = requireNonNull(reference, "reference is null");
        }

        @SuppressWarnings({"ObjectEquality", "ReferenceEquality"})
        public int compare(Block block, int position)
        {
            checkArgument(!block.isNull(position), "position is null");
            Slice next = VARCHAR.getSlice(block, position);
            // The expected case is a dictionary block with many entries for the
            // same path. Only perform a comparison if the object has changed.
            if (value != next) {
                value = next;
                result = value.compareTo(reference);
            }
            return result;
        }
    }
}
